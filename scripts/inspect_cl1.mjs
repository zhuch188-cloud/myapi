#!/usr/bin/env node
import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
if (!url || !authToken) {
  console.error("需要 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  process.exit(1);
}

const db = createClient({ url, authToken });
const sid = "CL1";

async function q(sql, args = []) {
  const r = await db.execute({ sql, args });
  return r.rows;
}

const cfg = (await q(
  "SELECT strategy_id, strategy_name, file_dir, file_name, status FROM strategy_configs WHERE strategy_id=?",
  [sid]
))[0];
console.log("=== config ===");
console.log(cfg);

const summary = (await q(
  `SELECT COUNT(*) AS rows, COUNT(DISTINCT rebalance_date) AS periods,
          MIN(rebalance_date) AS min_rb, MAX(rebalance_date) AS max_rb
   FROM strategy_positions WHERE strategy_id=?`,
  [sid]
))[0];
console.log("\n=== positions summary ===");
console.log(summary);

const perPeriod = await q(
  `SELECT rebalance_date, COUNT(*) AS stocks
   FROM strategy_positions WHERE strategy_id=?
   GROUP BY rebalance_date ORDER BY rebalance_date`,
  [sid]
);
console.log("\n=== stocks per period (first/last 3) ===");
perPeriod.slice(0, 3).forEach((r) => console.log(r));
console.log("...");
perPeriod.slice(-3).forEach((r) => console.log(r));
const stockCounts = perPeriod.map((r) => Number(r.stocks));
const avg = stockCounts.reduce((a, b) => a + b, 0) / (stockCounts.length || 1);
console.log(`avg stocks/period: ${avg.toFixed(1)} min=${Math.min(...stockCounts)} max=${Math.max(...stockCounts)}`);

const syncJobs = await q(
  `SELECT id, status, stage, import_mode, progress_at,
          substr(message,1,400) AS msg
   FROM admin_sync_jobs
   WHERE strategy_ids_json LIKE '%CL1%' OR message LIKE '%CL1%'
   ORDER BY id DESC LIMIT 10`
);
console.log("\n=== recent CL1 sync jobs ===");
syncJobs.forEach((r) => console.log(JSON.stringify(r)));

const impJobs = await q(
  `SELECT id, status, import_mode, progress_at,
          substr(message,1,400) AS msg,
          imported_count, failed_count,
          substr(completed_strategy_ids_json,1,200) AS done
   FROM strategy_import_jobs
   WHERE strategy_ids_json LIKE '%CL1%' OR message LIKE '%CL1%'
   ORDER BY id DESC LIMIT 10`
);
console.log("\n=== recent CL1 import jobs ===");
impJobs.forEach((r) => console.log(JSON.stringify(r)));

const sync8 = await q(
  `SELECT id, status, stage, checkpoint_json, result_json, substr(message,1,500) AS msg
   FROM admin_sync_jobs WHERE id=8`
);
console.log("\n=== sync job #8 detail ===");
console.log(JSON.stringify(sync8[0], null, 2));

// 估算：若全文件约 110 期（2016～2025 月频），每期 ~150 行
const estTotalRows = 110 * 150;
const estTotalPeriods = 110;
console.log("\n=== 估算（月频 2016～2025）===");
console.log(`文件约 ${estTotalPeriods} 期 × ~150 只 ≈ ${estTotalRows} 行`);
console.log(
  `库内已写入 ${summary.periods} 期 / ${summary.rows} 行，停在 ${summary.max_rb}`
);
console.log(
  `完成度约 ${((Number(summary.rows) / estTotalRows) * 100).toFixed(0)}%（按行数粗算）`
);

const allImp = await q(
  "SELECT id, status, import_mode, substr(message,1,100) AS m FROM strategy_import_jobs ORDER BY id"
);
console.log("\n=== 全部 strategy_import_jobs（看 import_mode 列）===");
allImp.forEach((r) => console.log(r));

const allSync = await q(
  "SELECT id, status, import_mode, stage, substr(message,1,100) AS m FROM admin_sync_jobs ORDER BY id"
);
console.log("\n=== 全部 admin_sync_jobs ===");
allSync.forEach((r) => console.log(r));
