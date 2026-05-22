import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
if (!url || !authToken) throw new Error("TURSO env required");

const db = createClient({ url, authToken });
const id = Number(process.argv[2] || 16);
const row = (
  await db.execute({
    sql: `SELECT id, status, import_mode, message, errors_json, checkpoint_json,
                 progress_at, finished_at, started_at
          FROM strategy_import_jobs WHERE id=?`,
    args: [id],
  })
).rows[0];
if (!row) {
  console.log("job not found", id);
  process.exit(1);
}
console.log(JSON.stringify(row, null, 2));
