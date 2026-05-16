# Strategy Showcase (Python)

Python B/S system for strategy presentation, authorization, and behavior tracking.

## Stack

- Backend: FastAPI
- DB: Turso (libSQL)
- Scheduler: APScheduler (daily `02:00` + manual trigger)
- Frontend: server-rendered HTML + responsive CSS

## Quick Start

### Windows (recommended on this machine)

PATH 上的 `python` 常为 Microsoft Store 占位符。本仓库用 **`scripts/Resolve-ProjectPython.ps1`** 按下面顺序解析解释器：

1. 环境变量 **`FINANCIAL_PRODUCT_BS_PYTHON`**（填完整 `python.exe` 路径）
2. 注册表 **`HKCU` / `HKLM`** 下 `SOFTWARE\Python\PythonCore\*\InstallPath` 已注册的安装（选版本号最高且存在 `python.exe` 的目录）
3. 回退为命令名 **`python`**（依赖 PATH）

在本机已检测到（供参考）：

| 路径 | 说明 |
|------|------|
| `C:\Users\hfjj\anaconda3\python.exe` | Conda base，注册表 Python 3.12，当前解析结果 |
| `C:\Users\hfjj\anaconda3\envs\openclaw\python.exe` | Conda 环境，Python 3.12 |
| `C:\Users\hfjj\anaconda3\envs\lang\python.exe` | Conda 环境，Python 3.8 |

依赖安装（使用同一解析逻辑）：

```powershell
$py = & .\scripts\Resolve-ProjectPython.ps1
& $py -m pip install -r requirements.txt
copy env.example .env
.\run.ps1
```

可选：用该解释器重建虚拟环境（避免复制来的 `.venv` 架构错误）：

```powershell
$py = & .\scripts\Resolve-ProjectPython.ps1
& $py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 通用（PATH 上已是真实 Python 时）

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy env.example .env
python run.py
```

Open: `http://localhost:8000`

Default users:

- admin / admin123
- editor / editor123
- viewer / viewer123

## Notes

- The app auto-creates DB/tables/indexes on startup.
- Strategy import root defaults to `D:\360云盘\指数部\策略研发与应用\展示策略` (override with `STRATEGY_ROOT_DIR` in `.env`).
- Holdings page size defaults to 20, options are 20/50/100.
