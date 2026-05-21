#!/usr/bin/env node
/** 复现 main._strategy_nav_list_summary_bounded / _nav_rolling_window_returns 的 SQL */
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

function pct(a, b) {
  return b > 0 ? ((a / b - 1) * 100).toFixed(4) + "%" : "n/a";
}

const top = (
  await db.execute({
    sql: `SELECT trade_date, nav_unit, daily_ret FROM strategy_nav_daily
          WHERE strategy_id = ? ORDER BY ${tdExpr} DESC LIMIT 1`,
    args: [sid],
  })
).rows[0];
const lastNav = Number(top.nav_unit);
const lastTd = String(top.trade_date).slice(0, 10);
const acmp = lastTd.replace(/-/g, "");

const off5 = (
  await db.execute({
    sql: `SELECT trade_date, nav_unit FROM strategy_nav_daily
          WHERE strategy_id = ? AND ${tdExpr} <= ? ORDER BY ${tdExpr} DESC LIMIT 1 OFFSET 5`,
    args: [sid, acmp],
  })
).rows[0];

const [y, m] = lastTd.split("-").map(Number);
const monthCut = `${y}${String(m).padStart(2, "0")}01`;
const yearCut = `${y}0101`;

const anchorM = (
  await db.execute({
    sql: `SELECT nav_unit, trade_date FROM strategy_nav_daily
          WHERE strategy_id = ? AND ${tdExpr} < ? ORDER BY ${tdExpr} DESC LIMIT 1`,
    args: [sid, monthCut],
  })
).rows[0];
const anchorY = (
  await db.execute({
    sql: `SELECT nav_unit, trade_date FROM strategy_nav_daily
          WHERE strategy_id = ? AND ${tdExpr} < ? ORDER BY ${tdExpr} DESC LIMIT 1`,
    args: [sid, yearCut],
  })
).rows[0];

console.log("末行:", lastTd, lastNav);
console.log("OFFSET 5:", off5?.trade_date, off5?.nav_unit, "→ 5日", pct(lastNav, Number(off5?.nav_unit)));
console.log("月锚:", anchorM?.trade_date, anchorM?.nav_unit, "→ 本月", pct(lastNav, Number(anchorM?.nav_unit)));
console.log("年锚:", anchorY?.trade_date, anchorY?.nav_unit, "→ 本年", pct(lastNav, Number(anchorY?.nav_unit)));

const cache = await db.execute({
  sql: "SELECT last_5d_return, month_return, year_return FROM strategy_list_metrics WHERE strategy_id=?",
  args: [sid],
});
const c = cache.rows[0];
const p = (x) => (x == null ? "null" : (Number(x) * 100).toFixed(4) + "%");
console.log("\n缓存:", p(c?.last_5d_return), p(c?.month_return), p(c?.year_return));
