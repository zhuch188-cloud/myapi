#!/usr/bin/env node
/** cs66 增量净值 bootstrap 诊断：持仓快照 vs 末净值 nav_unit */
import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
const sid = (process.argv[2] || "cs66").trim().toLowerCase();
const IC0 = Number(process.env.STRATEGY_NAV_INITIAL_CAPITAL || "100000000");

if (!url || !authToken) {
  console.error("需要 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  process.exit(1);
}

const db = createClient({ url, authToken });
async function q(sql, args = []) {
  return (await db.execute({ sql, args })).rows;
}

function cmpDate(s) {
  return String(s || "").slice(0, 10).replace(/-/g, "");
}

async function main() {
  const navLast = await q(
    `SELECT trade_date, nav_unit, rebalance_date
     FROM strategy_nav_daily WHERE strategy_id = ?
     ORDER BY trade_date DESC LIMIT 1`,
    [sid]
  );
  const nl = navLast[0];
  if (!nl) {
    console.log("无净值");
    process.exit(1);
  }
  const td = nl.trade_date;
  const tdCmp = cmpDate(td);
  const nuDb = Number(nl.nav_unit);
  const rb = nl.rebalance_date;
  console.log("末净值:", td, "nav_unit=", nuDb, "rb=", rb);
  console.log("名义本金 IC0=", IC0, "目标市值=", nuDb * IC0);

  const hold = await q(
    `SELECT COUNT(*) AS n, SUM(latest_weight) AS sw, SUM(period_weight) AS pw,
            SUM(COALESCE(latest_price,0) * COALESCE(latest_weight,0)) AS wpx
     FROM strategy_holding_daily
     WHERE strategy_id = ? AND (
       trade_date = ? OR REPLACE(trade_date,'-','') = ?
     ) AND (
       rebalance_date = ? OR REPLACE(rebalance_date,'-','') = REPLACE(?,'-','')
     )`,
    [sid, td, tdCmp, rb, rb]
  );
  console.log("\n末净值日持仓快照(同 rb):", hold[0]);

  const holdAny = await q(
    `SELECT COUNT(*) AS n, SUM(latest_weight) AS sw
     FROM strategy_holding_daily
     WHERE strategy_id = ? AND (
       trade_date = ? OR REPLACE(trade_date,'-','') = ?
     )`,
    [sid, td, tdCmp]
  );
  console.log("末净值日持仓(不限 rb):", holdAny[0]);

  const rbList = await q(
    `SELECT DISTINCT rebalance_date AS d FROM strategy_holding_daily
     WHERE strategy_id = ? ORDER BY d`,
    [sid]
  );
  console.log("holding_daily 调仓日:", rbList.map((r) => r.d).join(", ") || "(无)");

  const top = await q(
    `SELECT nav_unit FROM strategy_nav_daily WHERE strategy_id = ?
     ORDER BY trade_date DESC LIMIT 8`,
    [sid]
  );
  if (top.length >= 2) {
    const a = Number(top[0].nav_unit);
    const b = Number((top[5] || top[top.length - 1]).nav_unit);
    console.log("\n尺度断裂(末/第6):", a, "/", b, "=", (a / b).toFixed(4), a / b < 0.85 || a / b > 1.15 ? "***断裂***" : "正常");
  }

  const snap = await q(
    `SELECT stock_code, latest_weight, latest_price, period_weight
     FROM strategy_holding_daily
     WHERE strategy_id = ? AND (
       trade_date = ? OR REPLACE(trade_date,'-','') = ?
     ) AND rebalance_date = ?
     LIMIT 5`,
    [sid, td, tdCmp, rb]
  );
  console.log("\n快照样例(5行):");
  snap.forEach((r) => console.log(" ", r.stock_code, "lw=", r.latest_weight, "lp=", r.latest_price));

  const mvFromSnap = await q(
    `SELECT SUM(
       CASE WHEN latest_price IS NOT NULL AND latest_price > 0 AND latest_weight IS NOT NULL
       THEN latest_price * latest_weight ELSE 0 END
     ) AS mv_lp
     FROM strategy_holding_daily
     WHERE strategy_id = ? AND trade_date = ? AND rebalance_date = ?`,
    [sid, td, rb]
  );
  const mvLp = Number(mvFromSnap[0]?.mv_lp || 0);
  if (mvLp > 0 && nuDb > 0) {
    const nuCalc = mvLp / IC0;
    const rel = Math.abs(nuCalc - nuDb) / nuDb;
    console.log("\n用快照 latest_price×weight 粗算:");
    console.log("  mv=", mvLp, " nu_calc=", nuCalc.toFixed(6), " rel_err=", rel.toFixed(4), rel > 0.03 ? "***超3% tol***" : "在容差内");
  }

  const jobs = await q(
    `SELECT id, status, substr(message,1,400) AS msg FROM strategy_update_jobs ORDER BY id DESC LIMIT 3`
  );
  console.log("\n最近 jobs:");
  jobs.forEach((j) => console.log(` #${j.id} ${j.status}`, j.msg?.slice(0, 120)));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
