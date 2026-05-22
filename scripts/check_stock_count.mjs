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
const tdExpr = (c) => `REPLACE(SUBSTR(${c}, 1, 10), '-', '')`;

const metrics = (
  await db.execute({
    sql: "SELECT strategy_id, stock_count_on_last_date, last_trade_date FROM strategy_list_metrics WHERE strategy_id=?",
    args: [sid],
  })
).rows[0];
console.log("strategy_list_metrics:", metrics || "(无行)");

const nav = (
  await db.execute({
    sql: "SELECT trade_date, rebalance_date FROM strategy_nav_daily WHERE strategy_id=? ORDER BY trade_date DESC LIMIT 1",
    args: [sid],
  })
).rows[0];
console.log("last nav:", nav);

const hGroups = (
  await db.execute({
    sql: `SELECT trade_date, rebalance_date, COUNT(*) AS c FROM strategy_holding_daily
          WHERE strategy_id=? GROUP BY trade_date, rebalance_date ORDER BY trade_date DESC LIMIT 5`,
    args: [sid],
  })
).rows;
console.log("holding groups:", hGroups);

if (nav) {
  const td = String(nav.trade_date).slice(0, 10);
  const cnt = (
    await db.execute({
      sql: `SELECT COUNT(DISTINCT stock_code) AS c FROM strategy_holding_daily
            WHERE strategy_id=? AND ${tdExpr("trade_date")}=? AND ${tdExpr("rebalance_date")}=(
              SELECT MAX(${tdExpr("rebalance_date")}) FROM strategy_holding_daily
              WHERE strategy_id=? AND ${tdExpr("trade_date")}=?)`,
      args: [sid, td.replace(/-/g, ""), sid, td.replace(/-/g, "")],
    })
  ).rows[0];
  console.log("holdings count on last nav date (compact join):", cnt);
}
