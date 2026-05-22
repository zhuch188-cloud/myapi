#!/usr/bin/env node
import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
if (!url || !authToken) {
  console.error("需要 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  process.exit(1);
}

const db = createClient({ url, authToken });
const sid = process.argv[2] || "CL1";

const summary = (
  await db.execute({
    sql: `SELECT COUNT(*) AS rows, COUNT(DISTINCT rebalance_date) AS periods,
                 MIN(rebalance_date) AS min_rb, MAX(rebalance_date) AS max_rb
          FROM strategy_positions WHERE strategy_id=?`,
    args: [sid],
  })
).rows[0];

console.log("=== positions", sid, "===", summary);

const jobs = (
  await db.execute(
    `SELECT id, status, import_mode, progress_at, finished_at,
            imported_count, failed_count, substr(message,1,300) AS msg
     FROM strategy_import_jobs ORDER BY id`
  )
).rows;

console.log("\n=== strategy_import_jobs ===");
for (const j of jobs) {
  console.log(
    `#${j.id} ${j.status} ${j.import_mode} imported=${j.imported_count} @ ${j.progress_at}`,
  );
  console.log(" ", j.msg);
}

const per = (
  await db.execute({
    sql: `SELECT rebalance_date, COUNT(*) AS n FROM strategy_positions
          WHERE strategy_id=? GROUP BY rebalance_date ORDER BY rebalance_date`,
    args: [sid],
  })
).rows;

const counts = per.map((r) => Number(r.n));
const total = counts.reduce((a, b) => a + b, 0);
const low = per.filter((r) => Number(r.n) < 100);
console.log("\n=== period stats ===");
console.log("total rows", total, "periods", counts.length);
console.log("periods with <100 stocks:", low.length);
if (low.length) {
  console.log(
    "samples:",
    low
      .slice(0, 8)
      .map((r) => `${r.rebalance_date}:${r.n}`)
      .join(", "),
  );
}
