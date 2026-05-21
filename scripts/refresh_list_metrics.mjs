#!/usr/bin/env node
/** 全量刷新 strategy_list_metrics（需本机可 import app，或部署后由接口触发） */
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import path from "path";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const py = process.env.PYTHON || "python";
const r = spawnSync(
  py,
  [path.join(root, "scripts", "refresh_strategy_list_metrics.py"), "--apply"],
  { cwd: root, stdio: "inherit", env: process.env }
);
process.exit(r.status ?? 1);
