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
    # 定时数据更新：分 时 日 月 星期（APScheduler：星期一=0，周一至周五为 0-4）
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
    # 同步最后一步 run_update 前，若 _job_running 为真则轮询等待的最大秒数（定时/其它请求可能正占用）；0=不等待直接失败
    admin_sync_wait_idle_update_seconds: int = 180


settings = Settings()
