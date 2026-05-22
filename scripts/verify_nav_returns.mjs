#!/usr/bin/env node
/** 对照 daily_ret 与 nav 相邻日比值、各窗口收益率（需 TURSO_*） */
import { createClient } from "@libsql/client";

const sid = (process.argv[2] || "cs66").trim().toLowerCase();
const url = process.env.TURSO_DATABASE_URL;
const token = process.env.TURSO_AUTH_TOKEN;
if (!url || !token) {
  console.error("需要 TURSO_*");
  process.exit(1);
}
const db = createClient({ url, authToken: token });
const cmp = (s) => String(s || "").slice(0, 10).replace(/-/g, "");
const pct = (x) => (x == null ? "n/a" : (x * 100).toFixed(4) + "%");

const rows = (
  await db.execute({
    sql: `SELECT trade_date, nav_unit, daily_ret, rebalance_date
          FROM strategy_nav_daily WHERE strategy_id=? ORDER BY trade_date ASC`,
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
const lastTd = String(last.trade_date).slice(0, 10);
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
function navOffset(off) {
  return desc[off] ? Number(desc[off].nav_unit) : null;
}

console.log(`=== ${sid} 收益率口径对照（末 ${lastTd} nav=${lastNav.toFixed(6)}）===\n`);

console.log("【最近 5 个交易日】daily_ret vs nav[t]/nav[t-1]-1");
for (let i = 0; i < Math.min(5, desc.length); i++) {
  const r = desc[i];
  const nav = Number(r.nav_unit);
  const prev = desc[i + 1] ? Number(desc[i + 1].nav_unit) : null;
  const fromNav = prev > 0 ? nav / prev - 1 : null;
  const dr = r.daily_ret != null ? Number(r.daily_ret) : null;
  const diff =
    dr != null && fromNav != null ? ((dr - fromNav) * 10000).toFixed(2) + "bp" : "—";
  const rb = r.rebalance_date ? String(r.rebalance_date).slice(0, 10) : "";
  console.log(
    `  ${String(r.trade_date).slice(0, 10)} daily_ret=${pct(dr)} nav比=${pct(fromNav)} 差=${diff}${rb ? " 调仓=" + rb : ""}`
  );
}

const anchorM = navBefore(monthCut);
const anchorY = navBefore(yearCut);
const nav5 = navOffset(5);
console.log("\n【滚动窗口】末 nav / 锚定 nav - 1");
console.log(`  5日(前第5个交易日): ${pct(lastNav / nav5 - 1)}`);
console.log(`  本月(严格早于 ${monthCut}): ${pct(lastNav / anchorM - 1)}`);
console.log(`  本年(严格早于 ${yearCut}): ${pct(lastNav / anchorY - 1)}`);

console.log("\n【说明】");
console.log("  daily_ret 写入时 = 当日收盘市值/上一交易日收盘市值 - 1（非四舍五入 nav 比值）");
console.log("  调仓日 daily_ret 含收盘再平衡；与「旧持仓涨跌幅」可能不同");
console.log("  5日/本月/本年是单点比值，不等于 daily_ret 连乘");
