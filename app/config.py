from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8-sig")

    app_name: str = "Strategy Showcase (Python)"
    host: str = "0.0.0.0"
    port: int = 8000
    jwt_secret: str = "replace-this-in-prod"
    # 访问令牌有效期；配合 SlidingJWTAccessMiddleware：每次带有效 JWT 且响应成功时下发新令牌，实现「有操作自动续期」
    access_token_expire_minutes: int = 7 * 24 * 60

    # Turso（libSQL）应用主库
    turso_database_url: str = ""
    turso_auth_token: str = ""
    # 本地嵌入式副本路径（相对项目根）；留空则仅远程连接。副本会同步到 Turso 云端。
    turso_local_replica: str = ""

    # 远程 Wind（SQL Server，WindDB）。必填且须能连通；不再使用 MySQL winddb。
    wind_sqlserver_server: str = ""
    wind_sqlserver_port: int = 1433
    wind_sqlserver_database: str = "winddb"
    wind_sqlserver_user: str = ""
    wind_sqlserver_password: str = ""
    wind_sqlserver_driver: str = "ODBC Driver 17 for SQL Server"
    # 利润表 dbo.AShareIncome 中「归属母公司净利润」列名；留空则启动后首次查询时按库自动探测
    wind_income_net_profit_column: str = ""
    # 合并口径 STATEMENT_TYPE，逗号分隔、仅数字；9 位（如 408004000）与 12 位（408004000000）均可，留空用内置默认
    wind_income_statement_types: str = ""
    # dbo.AShareInsideHolder 中「股东排名」列名；留空则自动匹配候选列；仍无时按持股数量降序推断前十
    wind_inside_holder_rank_column: str = ""

    strategy_root_dir: str = r"D:\360云盘\指数部\策略研发与应用\展示策略"
    # Render 等部署：上传文件落盘目录（与 Turso 同机/同区导入）；留空则仅用本机 STRATEGY_ROOT_DIR
    server_upload_root: str = ""
    server_upload_max_mb: int = 200
    # 净值固定股数法：名义初始资金（元），仅用于市值→nav_unit 缩放；序列收益与绝对值无关，默认 1 亿
    strategy_nav_initial_capital: float = 100_000_000.0
    # 补充表导入文件路径；为空则使用 strategy_root_dir/数据/公司资料.xlsx
    supplement_company_excel_path: str = ""
    # 补充数据导入：每批 executemany 行数；纯远程 Turso 会自动压到 supplement_import_remote_batch_size
    supplement_import_batch_size: int = 200
    supplement_import_remote_batch_size: int = 50
    # RUNNING 超过该分钟数且 progress_at 未更新，视为僵死，可续传
    supplement_import_stale_running_minutes: int = 12
    # 定时数据更新：分 时 日 月 星期（APScheduler：星期一=0，周一至周五为 0-4；触发时区见 app.timeutil.BEIJING_TZ）
    daily_job_cron: str = "0 17 * * 0-4"
    # 火山方舟（豆包等）：个股 AI 摘要等使用。ARK_MODEL 为控制台「推理接入点」ID（ep-xxx）或模型名
    ark_api_key: str = ""
    ark_base_url: str = "https://ark.cn-beijing.volces.com/api/v3"
    ark_model: str = "ep-20260503233446-p4d2p"

    # 忘记密码邮件（不配 SMTP 时仍可提交申请；开发环境可开启 PASSWORD_RESET_RETURN_LINK_IN_JSON 在接口响应里带回重置链接）
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from_addr: str = ""
    # 465 等「全程 SSL」用 smtp_use_ssl=true；587 多为 STARTTLS（smtp_use_tls=true）
    smtp_use_ssl: bool = False
    smtp_use_tls: bool = True
    password_reset_public_base_url: str = ""
    password_reset_return_link_in_json: bool = False
    # 「联系我们」站内信收件箱（仅服务端使用，不在页面展示；可通过 .env 覆盖）
    contact_us_inbox_email: str = "zh1111@88.com"

    # 管理端「同步导入+净值+更新」：各阶段单独提交；阶段之间休眠秒数（0=不等待），减轻库与 Wind 并发压力
    admin_sync_sleep_after_import_seconds: float = 0.6
    admin_sync_sleep_after_nav_seconds: float = 0.6
    # strategy_update_jobs 仍为 RUNNING 且 started_at 早于此分钟数：视为僵尸（如进程崩溃未写 FAILED），同步前自动标 FAILED，避免永久 409
    stale_running_update_job_minutes: int = 240
    # admin_sync_jobs RUNNING 且 progress_at（无则 started_at）超过该分钟未更新：标 FAILED，可续传
    admin_sync_stale_progress_minutes: int = 30
    # 同步最后一步 run_update 前，若 _job_running 为真则轮询等待的最大秒数（定时/其它请求可能正占用）；0=不等待直接失败
    admin_sync_wait_idle_update_seconds: int = 180
    # Render 免费档等低内存环境：按策略串行拉 Wind 算净值/快照，不合并多策略 EOD（默认开；大内存本机可设 false）
    wind_low_memory_mode: bool = True
    # Wind 单次 IN 股票数（默认 80）；Render 512MB 建议 10～15（低内存未设时默认 15）
    wind_eod_stock_chunk: int = 0
    # 低内存：数据更新按「每个调仓期 × 该期成分股小批」拉 EOD 并立即落库（峰值≈单期持仓，Wind 重复读可接受）
    update_eod_per_rebalance_chunk: bool = True
    # 策略 Excel 导入：持仓 UPSERT 批大小（减少逐行往返）
    strategy_import_position_batch_size: int = 500
    # 大 Excel（如沪深300增强）：openpyxl 流式读行，避免整表进 pandas（阶段1 导入）
    strategy_excel_streaming_import: bool = True
    # 文件大于该 MB 时启用流式导入；0=始终流式
    strategy_excel_streaming_min_mb: int = 0
    # 流式 Excel 每批行数；Render 低内存建议 200～500（默认随 wind_low_memory_mode 自动缩小）
    strategy_excel_import_row_batch: int = 2500
    # 净值重建：每 N 个交易日落库一批（仅阶段2 nav_accum；不减小 day_map 峰值）
    nav_rebuild_persist_chunk: int = 400
    # 低内存下分段拉 Wind EOD 算净值（避免全区间 day_map 一次驻留）
    nav_rebuild_year_segments: bool = True
    # 净值 EOD 动态分段：最新一期成分股数 × 每段月数 <= budget（阶段2 开算前按 CL1 最新期成分重算）
    nav_rebuild_stock_month_budget: int = 300
    # 动态月数上限（0=不设顶，仅受 budget 约束）；对应环境变量 NAV_REBUILD_EOD_MONTHS_MAX
    nav_rebuild_eod_months_max: int = 0
    # 兼容旧配置：>0 且未设 MAX 时作为动态月数上限；不再固定为每段 1 个月
    nav_rebuild_eod_months: int = 0
    # 启动时跳过 Turso 日期格式一次性迁移（OOM 时可先 true 让服务起来，再本地跑 normalize_turso_dates.py）
    skip_startup_date_normalization: bool = False
    # 管理端 API 等待 Turso 流锁的最长时间（秒）；全量同步跑净值时其它接口可能排队
    turso_stream_lock_api_timeout_seconds: int = 90


settings = Settings()
