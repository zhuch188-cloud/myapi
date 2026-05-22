#!/usr/bin/env node
/** Excel 行数、唯一键、重复键统计（与导入解析一致） */
import { readFile } from "node:fs/promises";
import XLSX from "xlsx";

const path =
  process.argv[2] ||
  "D:\\展示策略\\展示策略\\展示策略-沪深300指数增强.xlsx";
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

const buf = await readFile(path);
const wb = XLSX.read(buf, { type: "buffer", cellDates: true });
const ws = wb.Sheets[wb.SheetNames[0]];
const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
const header = rows[0].slice(0, MAX_COL).map((c) => String(c ?? "").trim());
const iDt = header.indexOf("调整日期");
const iCode = header.indexOf("证券代码");
let total = 0;
let noCode = 0;
let noDate = 0;
let lastRb = null;
const keys = new Map();
const perRb = new Map();
for (let r = 1; r < rows.length; r++) {
  const cells = rows[r].slice(0, MAX_COL);
  let rb = parseDate(cells[iDt]);
  const code = normCode(cells[iCode]);
  if (!code) {
    noCode++;
    continue;
  }
  if (!rb) rb = lastRb;
  if (!rb) {
    noDate++;
    continue;
  }
  lastRb = rb;
  total++;
  const k = `${rb}|${code}`;
  keys.set(k, (keys.get(k) || 0) + 1);
  perRb.set(rb, (perRb.get(rb) || 0) + 1);
}
const dups = [...keys.entries()].filter(([, n]) => n > 1);
const dupRows = dups.reduce((a, [, n]) => a + (n - 1), 0);
console.log("file:", path);
console.log("sheet rows (incl header):", rows.length);
console.log("valid data rows (import rule):", total);
console.log("unique (rebalance_date, code):", keys.size);
console.log("duplicate key extra lines:", dupRows, "keys with dup:", dups.length);
console.log("skipped no code:", noCode, "skipped no date:", noDate);
console.log("periods:", perRb.size, "min", [...perRb.keys()].sort()[0], "max", [...perRb.keys()].sort().at(-1));
