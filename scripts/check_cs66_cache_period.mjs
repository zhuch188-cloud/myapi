#!/usr/bin/env node
import { createClient } from "@libsql/client";

const sid = "cs66";
const db = createClient({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const dup = await db.execute({
  sql: `SELECT trade_date, COUNT(*) AS c FROM strategy_nav_daily
        WHERE strategy_id = ? GROUP BY trade_date HAVING c > 1`,
  args: [sid],
});
console.log("dup dates:", dup.rows);

const cache = await db.execute({
  sql: `SELECT period_since_rebalance_return, last_5d_return, month_return, year_return
        FROM strategy_list_metrics WHERE strategy_id = ?`,
  args: [sid],
});
const c = cache.rows[0];
const p = (x) => (x == null ? "null" : (Number(x) * 100).toFixed(4) + "%");
console.log("cache period", p(c.period_since_rebalance_return));
console.log("cache 5d", p(c.last_5d_return));
console.log("cache month", p(c.month_return));
console.log("cache year", p(c.year_return));
