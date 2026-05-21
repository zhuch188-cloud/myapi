#!/usr/bin/env node
import { createClient } from "@libsql/client";

const sid = (process.argv[2] || "cs66").trim().toLowerCase();
const url = process.env.TURSO_DATABASE_URL;
const token = process.env.TURSO_AUTH_TOKEN;
if (!url || !token) {
  console.error("需要 TURSO_*");
  process.exit(1);
}
const db = createClient({ url, authToken: token });
const tdExpr = "REPLACE(SUBSTR(trade_date, 1, 10), '-', '')";

const last = (
  await db.execute({
    sql: `SELECT trade_date, nav_unit FROM strategy_nav_daily
          WHERE strategy_id=? ORDER BY trade_date DESC LIMIT 1`,
    args: [sid],
  })
).rows[0];
const acmp = String(last.trade_date).slice(0, 10).replace(/-/g, "");
const lastNav = Number(last.nav_unit);

for (const off of [0, 1, 2, 3, 4, 5]) {
  const lit = await db.execute({
    sql: `SELECT nav_unit FROM strategy_nav_daily
          WHERE strategy_id=? AND ${tdExpr} <= ?
          ORDER BY trade_date DESC LIMIT 1 OFFSET ${off}`,
    args: [sid, acmp],
  });
  const bind = await db.execute({
    sql: `SELECT nav_unit FROM strategy_nav_daily
          WHERE strategy_id=? AND ${tdExpr} <= ?
          ORDER BY trade_date DESC LIMIT 1 OFFSET ?`,
    args: [sid, acmp, off],
  });
  const nLit = Number(lit.rows[0]?.nav_unit);
  const nBind = Number(bind.rows[0]?.nav_unit);
  const ret = (b) => (b > 0 ? ((lastNav / b - 1) * 100).toFixed(4) + "%" : "n/a");
  console.log(
    `off=${off} literal=${nLit} (${ret(nLit)}) bind=${nBind} (${ret(nBind)}) match=${nLit === nBind}`
  );
}

const cache = (
  await db.execute({
    sql: `SELECT period_since_rebalance_return, last_5d_return, month_return, year_return
          FROM strategy_list_metrics WHERE strategy_id=?`,
    args: [sid],
  })
).rows[0];
console.log("\ncache:", cache);
if (cache?.period_since_rebalance_return != null) {
  console.log(
    "period as pct:",
    (Number(cache.period_since_rebalance_return) * 100).toFixed(4) + "%"
  );
}
