#!/usr/bin/env node
/**
 * 修正「末净值日持仓快照与净值尺度不一致」：
 * 删除指定策略的 strategy_holding_daily（让增量净值用 strategy_positions + Wind 对齐），
 * 并清理 strategy_list_metrics。
 *
 * 用法（需 TURSO_* 环境变量）：
 *   node scripts/fix_strategy_nav_bootstrap.mjs cs66
 *   node scripts/fix_strategy_nav_bootstrap.mjs cs66 --apply
 */
import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
const sid = (process.argv[2] || "").trim().toLowerCase();
const apply = process.argv.includes("--apply");

if (!url || !authToken) {
  console.error("需要 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  process.exit(1);
}
if (!sid) {
  console.error("用法: node scripts/fix_strategy_nav_bootstrap.mjs <strategy_id> [--apply]");
  process.exit(1);
}

const db = createClient({ url, authToken });

async function one(sql, args = []) {
  return (await db.execute({ sql, args })).rows;
}

async function main() {
  const cfg = await one(
    "SELECT strategy_id, strategy_name, status FROM strategy_configs WHERE LOWER(strategy_id)=LOWER(?)",
    [sid]
  );
  if (!cfg[0]) {
    console.error(`strategy_configs 无 ${sid}`);
    process.exit(1);
  }
  const canon = String(cfg[0].strategy_id);
  console.log(`策略: ${canon} (${cfg[0].strategy_name}) status=${cfg[0].status}`);

  const nav = await one(
    `SELECT trade_date, nav_unit, rebalance_date FROM strategy_nav_daily
     WHERE strategy_id=? ORDER BY trade_date DESC LIMIT 1`,
    [canon]
  );
  console.log("末净值:", nav[0] || "(无)");

  const hold = await one(
    `SELECT COUNT(*) AS c, MIN(trade_date) AS min_td, MAX(trade_date) AS max_td
     FROM strategy_holding_daily WHERE strategy_id=?`,
    [canon]
  );
  console.log("持仓快照:", hold[0] || {});

  const metrics = await one(
    "SELECT COUNT(*) AS c FROM strategy_list_metrics WHERE strategy_id=?",
    [canon]
  );
  console.log("列表指标缓存:", metrics[0]?.c ?? 0, "条");

  if (!apply) {
    console.log("\n预览：将 DELETE strategy_holding_daily + strategy_list_metrics");
    console.log("确认后加 --apply");
    return;
  }

  const d1 = await db.execute({
    sql: "DELETE FROM strategy_holding_daily WHERE strategy_id = ?",
    args: [canon],
  });
  const d2 = await db.execute({
    sql: "DELETE FROM strategy_list_metrics WHERE strategy_id = ?",
    args: [canon],
  });
  console.log(
    `\n已删除持仓 ${d1.rowsAffected ?? "?"} 行，列表快照 ${d2.rowsAffected ?? "?"} 条。`
  );
  console.log("末净值未改动。请在管理端对", canon, "勾选「全量更新净值」并更新持仓（需 Wind 可用）。");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
