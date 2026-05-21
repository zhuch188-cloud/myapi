#!/usr/bin/env node
/** 最少查询：仅 2~3 次 DELETE，无 SELECT / 无 SQLAlchemy 初始化 */
import { createClient } from "@libsql/client";
import { readFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const env = Object.fromEntries(
  readFileSync(resolve(root, ".env"), "utf8")
    .split("\n")
    .filter((l) => l.includes("=") && !l.trim().startsWith("#"))
    .map((l) => {
      const i = l.indexOf("=");
      let v = l.slice(i + 1).trim();
      if (
        (v.startsWith('"') && v.endsWith('"')) ||
        (v.startsWith("'") && v.endsWith("'"))
      ) {
        v = v.slice(1, -1);
      }
      return [l.slice(0, i).trim(), v];
    })
);

const positional = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const target = (positional[0] || "2026-05-21").slice(0, 10);
const compact = target.replace(/-/g, "");
const sid = (positional[1] || "").trim();

const db = createClient({
  url: env.TURSO_DATABASE_URL,
  authToken: env.TURSO_AUTH_TOKEN,
});

const tdExpr = "REPLACE(SUBSTR(trade_date,1,10),'-','')";
const sidClause = sid ? " AND strategy_id = ?" : "";
const args = sid ? [compact, sid] : [compact];

console.log(`删除 ${target} 净值/持仓（仅 DELETE，共 2~3 次请求）`);
if (sid) console.log(`策略: ${sid}`);

const navDel = await db.execute({
  sql: `DELETE FROM strategy_nav_daily WHERE ${tdExpr} = ?${sidClause}`,
  args,
});
const holdDel = await db.execute({
  sql: `DELETE FROM strategy_holding_daily WHERE ${tdExpr} = ?${sidClause}`,
  args,
});

let metricsN = 0;
try {
  const metricsDel = sid
    ? await db.execute({
        sql: "DELETE FROM strategy_list_metrics WHERE strategy_id = ?",
        args: [sid],
      })
    : await db.execute({ sql: "DELETE FROM strategy_list_metrics" });
  metricsN = metricsDel.rowsAffected ?? 0;
} catch (e) {
  const msg = String(e.message || e);
  if (!msg.includes("no such table")) throw e;
}

console.log(
  `完成: nav=${navDel.rowsAffected} hold=${holdDel.rowsAffected} list_metrics=${metricsN}`
);
