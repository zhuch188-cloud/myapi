#!/usr/bin/env node
/**
 * 对照 CL1 Excel 与 Turso（列：调整日期、证券代码，与导入逻辑一致）。
 * 用法: node scripts/compare_cl1_excel_db.mjs [excel路径]
 */
import { createClient } from "@libsql/client";
import { readFile } from "node:fs/promises";
import XLSX from "xlsx";

const EXCEL =
  process.argv[2] ||
  "D:\\展示策略\\展示策略\\展示策略-沪深300指数增强.xlsx";
const SID = "CL1";
const MAX_COL = 5;

function parseDate(v) {
  if (v == null || v === "") return null;
  if (v instanceof Date && !isNaN(v)) return v.toISOString().slice(0, 10);
  if (typeof v === "number") {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    const d = new Date(epoch.getTime() + v * 86400000);
    return d.toISOString().slice(0, 10);
  }
  const s = String(v).trim();
  if (!s) return null;
  const m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
  if (m) return `${m[1]}-${m[2].padStart(2, "0")}-${m[3].padStart(2, "0")}`;
  const d = new Date(s);
  if (!isNaN(d)) return d.toISOString().slice(0, 10);
  return null;
}

function normCode(raw) {
  if (raw == null || raw === "") return null;
  let t =
    typeof raw === "number"
      ? String(Number.isInteger(raw) ? raw : Math.trunc(raw))
      : String(raw).trim().toUpperCase();
  if (t.endsWith(".0") && /^[0-9]+\.0$/.test(t)) t = t.slice(0, -2);
  if (!t) return null;
  if (t.includes(".")) return t;
  if (t.length === 6 && t.startsWith("6")) return `${t}.SH`;
  if (t.length === 6) return `${t}.SZ`;
  return t;
}

async function excelPeriodCounts(path) {
  const buf = await readFile(path);
  const wb = XLSX.read(buf, { type: "buffer", cellDates: true });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
  if (!rows.length) throw new Error("空表");
  const header = rows[0].slice(0, MAX_COL).map((c) => String(c ?? "").trim());
  const iDt = header.indexOf("调整日期");
  const iCode = header.indexOf("证券代码");
  if (iDt < 0 || iCode < 0) throw new Error(`缺少列 header=${header.join(",")}`);
  const counts = new Map();
  let total = 0;
  let lastRb = null;
  for (let r = 1; r < rows.length; r++) {
    const cells = rows[r].slice(0, MAX_COL);
    let rb = parseDate(cells[iDt]);
    const code = normCode(cells[iCode]);
    if (!code) continue;
    if (!rb) rb = lastRb;
    if (!rb) continue;
    lastRb = rb;
    counts.set(rb, (counts.get(rb) || 0) + 1);
    total += 1;
  }
  return { counts, total };
}

async function dbPeriodCounts() {
  const url = process.env.TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;
  if (!url || !authToken) throw new Error("需要 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  const db = createClient({ url, authToken });
  const rows = await db.execute({
    sql: `SELECT rebalance_date AS rb, COUNT(*) AS cnt
          FROM strategy_positions WHERE strategy_id=? GROUP BY rebalance_date ORDER BY rb`,
    args: [SID],
  });
  const counts = new Map();
  let total = 0;
  let minRb = null;
  let maxRb = null;
  for (const r of rows.rows) {
    const rb = String(r.rb).slice(0, 10);
    const c = Number(r.cnt);
    counts.set(rb, c);
    total += c;
    if (!minRb || rb < minRb) minRb = rb;
    if (!maxRb || rb > maxRb) maxRb = rb;
  }
  return { counts, total, minRb, maxRb };
}

function cumThrough(counts, through) {
  let n = 0;
  for (const [rb, c] of counts) if (rb <= through) n += c;
  return n;
}

const ex = await excelPeriodCounts(EXCEL);
const db = await dbPeriodCounts();
const exDates = [...ex.counts.keys()].sort();
const dbDates = [...db.counts.keys()].sort();
const exMin = exDates[0];
const exMax = exDates[exDates.length - 1];

console.log("Excel:", EXCEL);
console.log("\n=== 汇总 ===");
console.log(
  `Excel: ${ex.total} 行, ${ex.counts.size} 期, ${exMin} ~ ${exMax}`
);
console.log(
  `Turso: ${db.total} 行, ${db.counts.size} 期, ${db.minRb} ~ ${db.maxRb}`
);

if (db.maxRb) {
  const expect = cumThrough(ex.counts, db.maxRb);
  console.log(
    `\n库内最后日 ${db.maxRb}：按 Excel 累计应约 ${expect} 行，实际 ${db.total}，差 ${expect - db.total}`
  );
}

const all = [...new Set([...ex.counts.keys(), ...db.counts.keys()])].sort();
const bad = [];
for (const rb of all) {
  const ec = ex.counts.get(rb) || 0;
  const dc = db.counts.get(rb) || 0;
  if (ec !== dc) bad.push({ rb, ec, dc });
}
console.log(`\n=== 调仓日不一致（${bad.length} 期）===`);
bad.slice(0, 20).forEach(({ rb, ec, dc }) =>
  console.log(`  ${rb}: Excel=${ec} DB=${dc} Δ=${dc - ec}`)
);
if (bad.length > 20) console.log(`  … 另有 ${bad.length - 20} 期`);

if (bad.length) {
  const first = bad[0].rb;
  console.log(`\n建议重导起点（首期不一致）: ${first}`);
  console.log(`  至该日 Excel 累计: ${cumThrough(ex.counts, first)} 行`);
}

if (db.maxRb && ex.counts.has(db.maxRb)) {
  console.log(
    `\n库内最后一期 ${db.maxRb}: Excel=${ex.counts.get(db.maxRb)} DB=${db.counts.get(db.maxRb) || 0}`
  );
}
