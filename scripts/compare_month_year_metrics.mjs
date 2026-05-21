#!/usr/bin/env node
/** 对照列表缓存 vs 手算 本月/本年/5日 */
import { createClient } from "@libsql/client";

const sid = (process.argv[2] || "cs66").trim().toLowerCase();
const url = process.env.TURSO_DATABASE_URL;
const token = process.env.TURSO_AUTH_TOKEN;
if (!url || !token) {
  console.error("需要 TURSO_*");
  process.exit(1);
}
const db = createClient({ url, authToken: token });

function cmp(s) {
  return String(s || "").slice(0, 10).replace(/-/g, "");
}
function asDate(s) {
  const t = String(s).slice(0, 10);
  return t.length >= 10 ? t : null;
}

const rows = (
  await db.execute({
    sql: `SELECT trade_date, nav_unit, daily_ret FROM strategy_nav_daily
          WHERE strategy_id = ? ORDER BY trade_date ASC`,
    args: [sid],
  })
).rows;
if (!rows.length) {
  console.log("无净值");
  process.exit(1);
}
const desc = [...rows].reverse();
const last = desc[0];
const lastNav = Number(last.nav_unit);
const lastTd = asDate(last.trade_date);
const [y, m] = lastTd.split("-").map(Number);
const monthCut = `${y}-${String(m).padStart(2, "0")}-01`;
const yearCut = `${y}-01-01`;

function navBefore(cutDate) {
  const c = cmp(cutDate);
  for (const r of desc) {
    if (cmp(r.trade_date) < c) return Number(r.nav_unit);
  }
  return null;
}
function firstOnOrAfter(d) {
  const c = cmp(d);
  for (const r of rows) {
    if (cmp(r.trade_date) >= c) return Number(r.nav_unit);
  }
  return null;
}
function ret(a, b) {
  return b > 0 ? ((a / b - 1) * 100).toFixed(4) + "%" : "n/a";
}

const anchorMonthEnd = navBefore(monthCut);
const anchorMonthFirst = firstOnOrAfter(monthCut);
const anchorYearEnd = navBefore(yearCut);
const nav5 = desc[5] ? Number(desc[5].nav_unit) : null;

console.log(`=== ${sid} 末净值 ${lastTd} nav=${lastNav.toFixed(6)} ===\n`);
console.log("手算（上月末锚定，与当前代码一致）:");
console.log(`  本月锚定(严格早于 ${monthCut}): ${anchorMonthEnd} → ${ret(lastNav, anchorMonthEnd)}`);
console.log(`  本年锚定(严格早于 ${yearCut}): ${anchorYearEnd} → ${ret(lastNav, anchorYearEnd)}`);
console.log(`  5日(前第5个交易日): ${nav5} → ${ret(lastNav, nav5)}`);
console.log("\n手算（备选：本月首个交易日为锚）:");
console.log(`  本月首日≥${monthCut}: ${anchorMonthFirst} → ${ret(lastNav, anchorMonthFirst)}`);

const cache = await db.execute({
  sql: `SELECT latest_nav, last_5d_return, month_return, year_return, last_trade_date
        FROM strategy_list_metrics WHERE strategy_id = ?`,
  args: [sid],
});
const c = cache.rows[0];
console.log("\nstrategy_list_metrics 缓存:");
if (!c) console.log("  (无缓存)");
else {
  const pct = (x) => (x == null ? "null" : (Number(x) * 100).toFixed(4) + "%");
  console.log(`  last_trade_date: ${c.last_trade_date}`);
  console.log(`  month: ${pct(c.month_return)}  year: ${pct(c.year_return)}  5d: ${pct(c.last_5d_return)}`);
}
console.log("\n近月净值:");
rows.filter((r) => cmp(r.trade_date) >= "20260425").forEach((r) => {
  console.log(`  ${r.trade_date} nav=${Number(r.nav_unit).toFixed(6)}`);
});
