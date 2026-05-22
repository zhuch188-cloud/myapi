import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
if (!url || !authToken) throw new Error("TURSO env required");

const db = createClient({ url, authToken });
const jobs = (
  await db.execute(
    `SELECT id, status, import_mode, message, errors_json, finished_at, progress_at,
            imported_count, checkpoint_json
     FROM strategy_import_jobs ORDER BY id DESC LIMIT 10`
  )
).rows;

console.log("=== recent jobs ===");
for (const j of jobs) {
  console.log(`#${j.id} ${j.status} ${j.import_mode} finished=${j.finished_at}`);
  console.log("  msg:", String(j.message || "").slice(0, 400));
  if (j.errors_json) console.log("  err:", j.errors_json);
}

const cl1 = (
  await db.execute({
    sql: `SELECT COUNT(*) AS n, COUNT(DISTINCT rebalance_date) AS p,
                 MIN(rebalance_date) AS mn, MAX(rebalance_date) AS mx
          FROM strategy_positions WHERE strategy_id = ?`,
    args: ["CL1"],
  })
).rows[0];
console.log("\n=== CL1 positions ===", cl1);
