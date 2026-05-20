#!/usr/bin/env node
import { createClient } from "@libsql/client";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
const sid = (process.argv[2] || "cs66").trim().toLowerCase();

if (!url || !authToken) {
  console.error("需要 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  process.exit(1);
}

const db = createClient({ url, authToken });

async function q(sql, args = []) {
  return (await db.execute({ sql, args })).rows;
}

async function main() {
  console.log("=== 各策略净值末日 ===\n");
  const all = await q(`
    SELECT strategy_id,
           COUNT(*) AS nav_rows,
           MIN(trade_date) AS min_td,
           MAX(trade_date) AS max_td
    FROM strategy_nav_daily
    GROUP BY strategy_id
    ORDER BY strategy_id
  `);
  for (const r of all) {
    console.log(
      `${r.strategy_id}: rows=${r.nav_rows} ${r.min_td} ~ ${r.max_td}`
    );
  }

  console.log(`\n=== ${sid} 2026-03-25 ~ 2026-05-25 净值 ===\n`);
  const tail = await q(
    `SELECT trade_date, nav_unit, daily_ret, rebalance_date
     FROM strategy_nav_daily
     WHERE strategy_id = ?
       AND trade_date >= '2026-03-25'
     ORDER BY trade_date`,
    [sid]
  );
  if (!tail.length) console.log("(无数据)");
  else tail.forEach((r) => console.log(`${r.trade_date} nav=${Number(r.nav_unit).toFixed(6)} ret=${r.daily_ret} rb=${r.rebalance_date}`));

  const gaps = await q(
    `SELECT trade_date FROM strategy_nav_daily
     WHERE strategy_id = ? AND trade_date >= '2026-04-01' AND trade_date <= '2026-05-25'
     ORDER BY trade_date`,
    [sid]
  );
  const last = gaps.length ? gaps[gaps.length - 1].trade_date : null;
  console.log(`\n${sid} >= 2026-04-01 共 ${gaps.length} 行，末日: ${last || "(无)"}`);

  console.log("\n=== 最近 strategy_update_jobs ===\n");
  const jobs = await q(
    `SELECT id, status, job_type, started_at, finished_at,
            substr(message, 1, 200) AS msg
     FROM strategy_update_jobs
     ORDER BY id DESC LIMIT 10`
  );
  jobs.forEach((j) =>
    console.log(`#${j.id} ${j.status} ${j.job_type} ${j.started_at} ~ ${j.finished_at || "-"}`)
  );
  if (jobs[0]?.msg) console.log("  msg:", jobs[0].msg);

  const hold = await q(
    `SELECT COUNT(*) AS c, MIN(trade_date) AS min_td, MAX(trade_date) AS max_td
     FROM strategy_holding_daily WHERE strategy_id = ?`,
    [sid]
  );
  console.log(`\n=== ${sid} strategy_holding_daily ===`, hold[0] || {});

  console.log("\n=== 最近 admin_sync_jobs ===\n");
  const syncs = await q(
    `SELECT id, status, sync_mode, created_at, finished_at,
            substr(progress_text, 1, 100) AS prog
     FROM admin_sync_jobs
     ORDER BY id DESC LIMIT 5`
  );
  syncs.forEach((s) =>
    console.log(`#${s.id} ${s.status} ${s.sync_mode} ${s.created_at}`)
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
