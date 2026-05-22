import { createClient } from "@libsql/client";
import { readFile } from "node:fs/promises";
import XLSX from "xlsx";

const EXCEL =
  process.argv[2] ||
  "D:\\展示策略\\展示策略\\展示策略-沪深300指数增强.xlsx";
const SID = "CL1";

function parseDate(v) {
  if (v == null || v === "") return null;
  if (v instanceof Date && !isNaN(v)) return v.toISOString().slice(0, 10);
  if (typeof v === "number") {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    return new Date(epoch.getTime() + v * 86400000).toISOString().slice(0, 10);
  }
  const s = String(v).trim();
  const m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
  if (m) return `${m[1]}-${m[2].padStart(2, "0")}-${m[3].padStart(2, "0")}`;
  const d = new Date(s);
  return isNaN(d) ? null : d.toISOString().slice(0, 10);
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

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
const db = createClient({ url, authToken });

const job = (
  await db.execute(
    "SELECT id, status, message, checkpoint_json, progress_at FROM strategy_import_jobs WHERE id=14"
  )
).rows[0];
console.log("=== job #14 ===");
console.log(job.status, job.progress_at);
console.log(job.message?.slice(-400));
if (job.checkpoint_json) console.log(JSON.parse(job.checkpoint_json));

const per = (
  await db.execute({
    sql: `SELECT rebalance_date AS rb, COUNT(*) AS n FROM strategy_positions
          WHERE strategy_id=? GROUP BY rebalance_date ORDER BY rb`,
    args: [SID],
  })
).rows;

const buf = await readFile(EXCEL);
const wb = XLSX.read(buf, { type: "buffer", cellDates: true });
const ws = wb.Sheets[wb.SheetNames[0]];
const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
const header = rows[0].slice(0, 5).map((c) => String(c ?? "").trim());
const iDt = header.indexOf("调整日期");
const iCode = header.indexOf("证券代码");
const exPer = new Map();
let lastRb = null;
for (let r = 1; r < rows.length; r++) {
  const cells = rows[r].slice(0, 5);
  let rb = parseDate(cells[iDt]);
  const code = normCode(cells[iCode]);
  if (!code) continue;
  if (!rb) rb = lastRb;
  if (!rb) continue;
  lastRb = rb;
  exPer.set(rb, (exPer.get(rb) || 0) + 1);
}

let dbCum = 0;
let exCum = 0;
const bad = [];
for (const [rb, ec] of [...exPer.entries()].sort()) {
  exCum += ec;
  const dr = per.find((p) => String(p.rb).slice(0, 10) === rb);
  const dc = dr ? Number(dr.n) : 0;
  dbCum += dc;
  if (ec !== dc) bad.push({ rb, ec, dc, exCum, dbCum, gap: exCum - dbCum });
}

console.log("\n=== first mismatch periods (cumulative gap) ===");
bad.slice(0, 15).forEach((b) =>
  console.log(
    `${b.rb}: Excel期${b.ec} DB期${b.dc} | 累计 Excel ${b.exCum} DB ${b.dbCum} 差 ${b.gap}`
  )
);
console.log("\nDB periods:", per.length, "sum", per.reduce((a, p) => a + Number(p.n), 0));
console.log("Excel periods:", exPer.size, "sum", [...exPer.values()].reduce((a, b) => a + b, 0));
