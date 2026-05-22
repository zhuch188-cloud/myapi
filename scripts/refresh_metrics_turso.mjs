#!/usr/bin/env node
/** Turso 上刷新 strategy_list_metrics（本月/本年/5日 与列表口径一致） */
import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const token = process.env.TURSO_AUTH_TOKEN;
const apply = process.argv.includes("--apply");
if (!url || !token) {
  console.error("需要 TURSO_*");
  process.exit(1);
}
const db = createClient({ url, authToken: token });
const tdExpr = "REPLACE(SUBSTR(trade_date, 1, 10), '-', '')";

function asDate(s) {
  return String(s || "").slice(0, 10);
}
function cmp(s) {
  return String(s).slice(0, 10).replace(/-/g, "");
}

async function summaryFor(sid) {
  const rows = (
    await db.execute({
      sql: `SELECT trade_date, nav_unit, daily_ret, rebalance_date FROM strategy_nav_daily
            WHERE strategy_id = ? ORDER BY trade_date ASC`,
      args: [sid],
    })
  ).rows;
  if (!rows.length) return null;
  const desc = [...rows].reverse();
  const last = desc[0];
  const lastNav = Number(last.nav_unit);
  const lastTd = asDate(last.trade_date);
  const [y, m] = lastTd.split("-").map(Number);
  const monthCut = `${y}-${String(m).padStart(2, "0")}-01`;
  const yearCut = `${y}-01-01`;
  let anchorM = null;
  let anchorY = null;
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
  const nav5 = desc[5] ? Number(desc[5].nav_unit) : null;
  let last1d = last.daily_ret != null ? Number(last.daily_ret) : null;

  // 本期：锚定调仓 2026-05-06 首期
  const rbRows = (
    await db.execute({
      sql: `SELECT DISTINCT rebalance_date FROM strategy_positions WHERE strategy_id=?`,
      args: [sid],
    })
  ).rows.map((r) => asDate(r.rebalance_date)).filter(Boolean).sort();
  let periodRb = null;
  for (const rb of rbRows) {
    if (rb <= lastTd) periodRb = rb;
  }
  let p0 = null;
  if (periodRb) {
    for (const r of rows) {
      if (asDate(r.rebalance_date) === periodRb) {
        p0 = Number(r.nav_unit);
        break;
      }
    }
    if (p0 == null) {
      for (const r of rows) {
        if (cmp(r.trade_date) >= cmp(periodRb)) {
          p0 = Number(r.nav_unit);
          break;
        }
      }
    }
  }
  const ret = (a, b) => (b > 0 ? a / b - 1 : null);
  const tdCmp = cmp(lastTd);
  const stockCnt = (
    await db.execute({
      sql: `SELECT COUNT(DISTINCT h.stock_code) AS c
            FROM strategy_holding_daily h
            WHERE h.strategy_id = ?
              AND REPLACE(SUBSTR(h.trade_date, 1, 10), '-', '') = ?
              AND REPLACE(SUBSTR(h.rebalance_date, 1, 10), '-', '') = (
                SELECT MAX(REPLACE(SUBSTR(x.rebalance_date, 1, 10), '-', ''))
                FROM strategy_holding_daily x
                WHERE x.strategy_id = ?
                  AND REPLACE(SUBSTR(x.trade_date, 1, 10), '-', '') = ?
              )`,
      args: [sid, tdCmp, sid, tdCmp],
    })
  ).rows[0]?.c;
  return {
    latest_nav: Math.round(lastNav * 100) / 100,
    last_1d: last1d,
    last_5d: nav5 > 0 ? ret(lastNav, nav5) : null,
    period: p0 > 0 ? ret(lastNav, p0) : null,
    month: anchorM > 0 ? ret(lastNav, anchorM) : null,
    year: anchorY > 0 ? ret(lastNav, anchorY) : null,
    last_td: lastTd,
    period_rb: periodRb,
    stock_count: stockCnt != null ? Number(stockCnt) : null,
  };
}

const sids = (
  await db.execute(
    `SELECT strategy_id FROM strategy_configs WHERE status='enabled' AND is_visible=1`
  )
).rows.map((r) => r.strategy_id);

console.log(`将刷新 ${sids.length} 个策略\n`);
for (const sid of sids) {
  const s = await summaryFor(sid);
  if (!s) {
    console.log(`${sid}: 无净值，跳过`);
    continue;
  }
  const pct = (x) => (x == null ? "null" : (x * 100).toFixed(2) + "%");
  console.log(
    `${sid} 末${s.last_td} | 本月${pct(s.month)} 本年${pct(s.year)} 5日${pct(s.last_5d)} 本期${pct(s.period)}`
  );
  if (apply) {
    await db.execute({
      sql: `INSERT INTO strategy_list_metrics (
        strategy_id, latest_nav, last_1d_return, last_5d_return,
        period_since_rebalance_return, month_return, year_return,
        last_trade_date, stock_count_on_last_date, period_rebalance_date, updated_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now','+8 hours'))
      ON CONFLICT(strategy_id) DO UPDATE SET
        latest_nav=excluded.latest_nav,
        last_1d_return=excluded.last_1d_return,
        last_5d_return=excluded.last_5d_return,
        period_since_rebalance_return=excluded.period_since_rebalance_return,
        month_return=excluded.month_return,
        year_return=excluded.year_return,
        last_trade_date=excluded.last_trade_date,
        stock_count_on_last_date=excluded.stock_count_on_last_date,
        period_rebalance_date=excluded.period_rebalance_date,
        updated_at=excluded.updated_at`,
      args: [
        sid,
        s.latest_nav,
        s.last_1d,
        s.last_5d,
        s.period,
        s.month,
        s.year,
        s.last_td,
        s.stock_count,
        s.period_rb,
      ],
    });
  }
}
if (!apply) console.log("\n预览；加 --apply 写入");
