#!/usr/bin/env node
/** 按指定日历日删除净值（及同日持仓、列表快照）。用法: node scripts/purge_nav_on_date.mjs cs66 2026-05-21 [--apply] */
import { createClient } from "@libsql/client";

const sid = (process.argv[2] || "").trim().toLowerCase();
const onDate = (process.argv[3] || "").trim().slice(0, 10);
const apply = process.argv.includes("--apply");

if (!sid || !/^\d{4}-\d{2}-\d{2}$/.test(onDate)) {
  console.error("用法: node scripts/purge_nav_on_date.mjs <strategy_id> YYYY-MM-DD [--apply]");
  process.exit(1);
}
const onCmp = onDate.replace(/-/g, "");
const url = process.env.TURSO_DATABASE_URL;
const token = process.env.TURSO_AUTH_TOKEN;
if (!url || !token) {
  console.error("需要 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  process.exit(1);
}
const db = createClient({ url, authToken: token });

const cfg = await db.execute({
  sql: "SELECT strategy_id, strategy_name FROM strategy_configs WHERE LOWER(strategy_id)=LOWER(?)",
  args: [sid],
});
if (!cfg.rows[0]) {
  console.error(`无策略 ${sid}`);
  process.exit(1);
}
const canon = String(cfg.rows[0].strategy_id);
console.log(`策略: ${canon} (${cfg.rows[0].strategy_name})`);
console.log(`目标日: ${onDate}（仅删该日，不删其它日期）\n`);

const navRows = await db.execute({
  sql: `SELECT trade_date, nav_unit, daily_ret, rebalance_date FROM strategy_nav_daily
        WHERE strategy_id = ? AND (trade_date = ? OR trade_date = ?)`,
  args: [canon, onDate, onCmp],
});
const holdCnt = await db.execute({
  sql: `SELECT COUNT(*) AS c FROM strategy_holding_daily
        WHERE strategy_id = ? AND (trade_date = ? OR trade_date = ?)`,
  args: [canon, onDate, onCmp],
});
const tail = await db.execute({
  sql: `SELECT trade_date, nav_unit FROM strategy_nav_daily WHERE strategy_id = ?
        ORDER BY trade_date DESC LIMIT 3`,
  args: [canon],
});

console.log(`将删 strategy_nav_daily: ${navRows.rows.length} 行`);
navRows.rows.forEach((r) =>
  console.log(`  ${r.trade_date} nav=${r.nav_unit} 1d=${r.daily_ret} rb=${r.rebalance_date}`)
);
console.log(`将删 strategy_holding_daily: ${holdCnt.rows[0]?.c ?? 0} 行`);
console.log("\n删前末 3 日净值:");
[...tail.rows].reverse().forEach((r) => console.log(`  ${r.trade_date} nav=${r.nav_unit}`));

if (!apply) {
  console.log("\n预览完成。确认后加 --apply");
  process.exit(0);
}
if (navRows.rows.length === 0) {
  console.log("\n无匹配净值，未执行删除。");
  process.exit(0);
}
await db.execute({
  sql: `DELETE FROM strategy_nav_daily WHERE strategy_id = ? AND (trade_date = ? OR trade_date = ?)`,
  args: [canon, onDate, onCmp],
});
await db.execute({
  sql: `DELETE FROM strategy_holding_daily WHERE strategy_id = ? AND (trade_date = ? OR trade_date = ?)`,
  args: [canon, onDate, onCmp],
});
const m = await db.execute({
  sql: "DELETE FROM strategy_list_metrics WHERE strategy_id = ?",
  args: [canon],
});
const after = await db.execute({
  sql: `SELECT trade_date, nav_unit FROM strategy_nav_daily WHERE strategy_id = ?
        ORDER BY trade_date DESC LIMIT 1`,
  args: [canon],
});
console.log(`\n已删除。列表快照清理 ${m.rowsAffected ?? 0} 条。`);
console.log("删后末净值:", after.rows[0] || "(无)");
