#!/usr/bin/env node
/** 模拟净值页默认「最近一年」筛选下的 nav-metrics 口径 */
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

function cmp(s) {
  return String(s).slice(0, 10).replace(/-/g, "");
}
function asDate(s) {
  return String(s).slice(0, 10);
}

const end = new Date();
const start = new Date(end.getFullYear() - 1, end.getMonth(), end.getDate());
const sd = end.toLocaleDateString("en-CA", { timeZone: "Asia/Shanghai" }).replace(
  /(\d+)-(\d+)-(\d+)/,
  (_, y, m, d) => `${y}-${m}-${d}`
);
// fix start calc
const startStr = new Date(end.getTime() - 365 * 86400000).toLocaleDateString("en-CA", {
  timeZone: "Asia/Shanghai",
});

const all = (
  await db.execute({
    sql: `SELECT trade_date, nav_unit, daily_ret FROM strategy_nav_daily
          WHERE strategy_id=? ORDER BY trade_date ASC`,
    args: [sid],
  })
).rows;
const filtered = all.filter((r) => asDate(r.trade_date) >= startStr);
const last = filtered[filtered.length - 1];
const lastNav = Number(last.nav_unit);
const lastTd = asDate(last.trade_date);

const desc = [...all].reverse();
const top = desc[0];
const gNav = Number(top.nav_unit);
const gTd = asDate(top.trade_date);

const [y, m] = gTd.split("-").map(Number);
const monthCut = `${y}-${String(m).padStart(2, "0")}-01`;
const yearCut = `${y}-01-01`;
let anchorM = null,
  anchorY = null,
  nav5 = null;
for (const r of desc) {
  if (cmp(r.trade_date) < cmp(monthCut)) {
    anchorM = Number(r.nav_unit);
    break;
  }
}
for (const r of desc) {
  if (cmp(r.trade_date) < cmp(yearCut)) {
    anchorY = Number(r.nav_unit);
    break;
  }
}
if (desc[5]) nav5 = Number(desc[5].nav_unit);

const pct = (a, b) => (b > 0 ? ((a / b - 1) * 100).toFixed(2) + "%" : "n/a");

console.log("策略:", sid);
console.log("筛选: 最近一年", startStr, "~", end.toLocaleDateString("en-CA", { timeZone: "Asia/Shanghai" }));
console.log("区间内末日:", lastTd, "nav=", lastNav.toFixed(4));
console.log("库内最新:", gTd, "nav=", gNav.toFixed(4));
console.log("\n【应显示】按库内最新（新代码）:");
console.log("  近5交易日:", pct(gNav, nav5));
console.log("  本月:", pct(gNav, anchorM));
console.log("  本年:", pct(gNav, anchorY));
console.log("\n【易错】若仍用区间内末日（旧代码）:");
console.log("  近5交易日:", pct(lastNav, nav5));
console.log("  本月:", pct(lastNav, anchorM));

const cache = await db.execute({
  sql: "SELECT month_return, year_return, last_5d_return FROM strategy_list_metrics WHERE strategy_id=?",
  args: [sid],
});
if (cache.rows[0]) {
  const c = cache.rows[0];
  const p = (x) => (x == null ? "null" : (Number(x) * 100).toFixed(2) + "%");
  console.log("\n策略列表缓存:");
  console.log("  5日:", p(c.last_5d_return), " 本月:", p(c.month_return), " 本年:", p(c.year_return));
}
