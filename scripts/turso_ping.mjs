#!/usr/bin/env node
/** 一次性 Turso 连通与策略抽查；凭据仅读环境变量，勿写入仓库。 */
import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
const sid = (process.argv[2] || "CS66").trim();

if (!url || !authToken) {
  console.error("需要环境变量 TURSO_DATABASE_URL 与 TURSO_AUTH_TOKEN");
  process.exit(1);
}

const db = createClient({ url, authToken });

async function one(sql, args = []) {
  const r = await db.execute({ sql, args });
  return r.rows;
}

async function main() {
  const ping = await one("SELECT 1 AS ok");
  console.log("=== 连接 ===");
  console.log("ok:", ping[0]?.ok);
  console.log("url:", url.replace(/\/\/[^@]+@/, "//***@"));

  const cfg = await one(
    "SELECT strategy_id, strategy_name, status, file_dir, file_name FROM strategy_configs WHERE strategy_id = ?",
    [sid]
  );
  console.log(`\n=== strategy_configs (${sid}) ===`);
  console.log(cfg[0] || "(无记录)");

  const pos = await one(
    `SELECT COUNT(*) AS rows,
            COUNT(DISTINCT rebalance_date) AS periods,
            MIN(rebalance_date) AS min_rb,
            MAX(rebalance_date) AS max_rb
     FROM strategy_positions WHERE strategy_id = ?`,
    [sid]
  );
  console.log(`\n=== strategy_positions ===`);
  console.log(pos[0] || {});

  const rbList = await one(
    `SELECT DISTINCT rebalance_date AS d FROM strategy_positions
     WHERE strategy_id = ? ORDER BY d DESC LIMIT 12`,
    [sid]
  );
  if (rbList.length) {
    console.log("最近调仓日( positions ):");
    rbList.forEach((r) => console.log(" ", r.d));
  }

  const nav = await one(
    `SELECT COUNT(*) AS rows, MIN(trade_date) AS min_td, MAX(trade_date) AS max_td
     FROM strategy_nav_daily WHERE strategy_id = ?`,
    [sid]
  );
  console.log("\n=== strategy_nav_daily ===");
  console.log(nav[0] || {});

  const hold = await one(
    `SELECT COUNT(*) AS rows, MIN(trade_date) AS min_td, MAX(trade_date) AS max_td
     FROM strategy_holding_daily WHERE strategy_id = ?`,
    [sid]
  );
  console.log("\n=== strategy_holding_daily ===");
  console.log(hold[0] || {});

  if (Number(nav[0]?.rows || 0) > 0) {
    const tail = await one(
      `SELECT trade_date, nav_unit, daily_ret, rebalance_date
       FROM strategy_nav_daily WHERE strategy_id = ?
       ORDER BY trade_date DESC LIMIT 5`,
      [sid]
    );
    console.log("\n=== 净值末 5 行 ===");
    [...tail].reverse().forEach((r) => {
      console.log(
        `  ${r.trade_date} nav=${r.nav_unit} ret=${r.daily_ret} rb=${r.rebalance_date}`
      );
    });
  }

  const allStrategies = await one(
    `SELECT strategy_id, strategy_name, status FROM strategy_configs
     WHERE strategy_id LIKE '%66%' OR strategy_id LIKE '%CS%' ORDER BY strategy_id`
  );
  if (allStrategies.length) {
    console.log("\n=== 含 CS/66 的策略 ID ===");
    allStrategies.forEach((r) => console.log(`  ${r.strategy_id} ${r.strategy_name} ${r.status}`));
  }
}

main().catch((e) => {
  console.error("连接失败:", e.message || e);
  process.exit(1);
});
