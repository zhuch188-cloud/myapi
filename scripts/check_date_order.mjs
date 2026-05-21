#!/usr/bin/env node
import { createClient } from "@libsql/client";

const sid = "cs66";
const db = createClient({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const nonIso = await db.execute({
  sql: `SELECT trade_date FROM strategy_nav_daily
        WHERE strategy_id = ? AND trade_date NOT GLOB '????-??-??'`,
  args: [sid],
});
console.log("non-iso count:", nonIso.rows.length);

const bad = await db.execute({
  sql: `SELECT trade_date, nav_unit FROM strategy_nav_daily
        WHERE strategy_id = ? ORDER BY trade_date DESC LIMIT 8`,
  args: [sid],
});
console.log("\nORDER BY trade_date DESC (lex):");
for (const r of bad.rows) console.log(r.trade_date, r.nav_unit);

const good = await db.execute({
  sql: `SELECT trade_date, nav_unit FROM strategy_nav_daily
        WHERE strategy_id = ? ORDER BY REPLACE(SUBSTR(trade_date,1,10),'-','') DESC LIMIT 8`,
  args: [sid],
});
console.log("\nORDER BY compact DESC:");
for (const r of good.rows) console.log(r.trade_date, r.nav_unit);

const offLex = await db.execute({
  sql: `SELECT trade_date, nav_unit FROM strategy_nav_daily
        WHERE strategy_id = ? ORDER BY trade_date DESC LIMIT 1 OFFSET 5`,
  args: [sid],
});
console.log("\nOFFSET 5 lex:", offLex.rows[0]?.trade_date, offLex.rows[0]?.nav_unit);
