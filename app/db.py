import json
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from app.config import settings


BASE_URL = URL.create(
    "mysql+pymysql",
    username=settings.db_user,
    password=settings.db_password,
    host=settings.db_host,
    port=settings.db_port,
)
DB_URL = URL.create(
    "mysql+pymysql",
    username=settings.db_user,
    password=settings.db_password,
    host=settings.db_host,
    port=settings.db_port,
    database=settings.db_name,
    query={"charset": "utf8mb4"},
)


def init_database() -> None:
    base_engine = create_engine(BASE_URL, pool_pre_ping=True)
    with base_engine.connect() as conn:
        conn.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{settings.db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
        )
        conn.commit()

    engine = create_engine(DB_URL, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    username VARCHAR(64) UNIQUE NOT NULL,
                    password VARCHAR(255) NOT NULL,
                    role ENUM('admin','editor','viewer') NOT NULL,
                    org_id VARCHAR(64) NOT NULL,
                    status ENUM('active','disabled','locked') NOT NULL DEFAULT 'active',
                    password_is_system_generated TINYINT(1) NOT NULL DEFAULT 1,
                    password_changed_at DATETIME NULL,
                    failed_login_count INT NOT NULL DEFAULT 0,
                    locked_until DATETIME NULL,
                    last_login_at DATETIME NULL,
                    last_login_ip VARCHAR(64) NULL,
                    nickname VARCHAR(64) NULL,
                    contact_phone VARCHAR(32) NULL,
                    contact_email VARCHAR(255) NULL,
                    profile_bio VARCHAR(500) NULL,
                    UNIQUE KEY uk_users_nickname (nickname),
                    UNIQUE KEY uk_users_contact_phone (contact_phone),
                    UNIQUE KEY uk_users_contact_email (contact_email),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
            )
        )
        user_cols = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'users'
                """
            )
        ).mappings().all()
        user_set = {str(r["COLUMN_NAME"]).lower() for r in user_cols}
        if "status" not in user_set:
            conn.execute(
                text(
                    "ALTER TABLE users ADD COLUMN status ENUM('active','disabled','locked') NOT NULL DEFAULT 'active'"
                )
            )
        if "password_is_system_generated" not in user_set:
            conn.execute(
                text(
                    "ALTER TABLE users ADD COLUMN password_is_system_generated TINYINT(1) NOT NULL DEFAULT 1"
                )
            )
        if "password_changed_at" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN password_changed_at DATETIME NULL"))
        if "failed_login_count" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN failed_login_count INT NOT NULL DEFAULT 0"))
        if "locked_until" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN locked_until DATETIME NULL"))
        if "last_login_at" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN last_login_at DATETIME NULL"))
        if "last_login_ip" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN last_login_ip VARCHAR(64) NULL"))
        if "created_at" not in user_set:
            conn.execute(
                text("ALTER TABLE users ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP")
            )
        if "updated_at" not in user_set:
            conn.execute(
                text(
                    "ALTER TABLE users ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                )
            )
        if "nickname" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN nickname VARCHAR(64) NULL"))
        if "contact_phone" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN contact_phone VARCHAR(32) NULL"))
        if "contact_email" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN contact_email VARCHAR(255) NULL"))
        if "profile_bio" not in user_set:
            conn.execute(text("ALTER TABLE users ADD COLUMN profile_bio VARCHAR(500) NULL"))
        # 历史数据兼容：空字符串统一归零值，避免唯一索引冲突
        conn.execute(text("UPDATE users SET nickname=NULL WHERE nickname IS NOT NULL AND TRIM(nickname)=''"))
        conn.execute(text("UPDATE users SET contact_phone=NULL WHERE contact_phone IS NOT NULL AND TRIM(contact_phone)=''"))
        conn.execute(text("UPDATE users SET contact_email=NULL WHERE contact_email IS NOT NULL AND TRIM(contact_email)=''"))
        idx_rows = conn.execute(
            text(
                """
                SELECT index_name
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'users'
                  AND non_unique = 0
                """
            )
        ).mappings().all()
        idx_set = {
            str(r.get("index_name") or r.get("INDEX_NAME") or "").lower()
            for r in idx_rows
            if (r.get("index_name") or r.get("INDEX_NAME"))
        }
        dup_nk = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM (
                  SELECT nickname
                  FROM users
                  WHERE nickname IS NOT NULL
                  GROUP BY nickname
                  HAVING COUNT(*) > 1
                ) t
                """
            )
        ).mappings().first()
        dup_ph = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM (
                  SELECT contact_phone
                  FROM users
                  WHERE contact_phone IS NOT NULL
                  GROUP BY contact_phone
                  HAVING COUNT(*) > 1
                ) t
                """
            )
        ).mappings().first()
        dup_em = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM (
                  SELECT contact_email
                  FROM users
                  WHERE contact_email IS NOT NULL
                  GROUP BY contact_email
                  HAVING COUNT(*) > 1
                ) t
                """
            )
        ).mappings().first()
        if "uk_users_nickname" not in idx_set and int((dup_nk or {}).get("c") or 0) == 0:
            conn.execute(text("ALTER TABLE users ADD UNIQUE KEY uk_users_nickname (nickname)"))
        if "uk_users_contact_phone" not in idx_set and int((dup_ph or {}).get("c") or 0) == 0:
            conn.execute(text("ALTER TABLE users ADD UNIQUE KEY uk_users_contact_phone (contact_phone)"))
        if "uk_users_contact_email" not in idx_set and int((dup_em or {}).get("c") or 0) == 0:
            conn.execute(text("ALTER TABLE users ADD UNIQUE KEY uk_users_contact_email (contact_email)"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_devices (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    user_id BIGINT NOT NULL,
                    device_token_hash CHAR(64) NOT NULL,
                    device_token_expires_at DATETIME NOT NULL,
                    trusted TINYINT(1) NOT NULL DEFAULT 1,
                    device_name VARCHAR(128) NULL,
                    ua VARCHAR(512) NULL,
                    platform VARCHAR(128) NULL,
                    ip_first VARCHAR(64) NULL,
                    ip_last VARCHAR(64) NULL,
                    last_seen_at DATETIME NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    revoked_at DATETIME NULL,
                    UNIQUE KEY uk_device_token_hash (device_token_hash),
                    KEY idx_user_devices_user (user_id),
                    KEY idx_user_devices_expires (device_token_expires_at)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    actor_user_id BIGINT NULL,
                    action VARCHAR(128) NOT NULL,
                    target_user_id BIGINT NULL,
                    detail_json JSON NULL,
                    ip VARCHAR(64) NULL,
                    ua VARCHAR(512) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_audit_target (target_user_id),
                    KEY idx_audit_actor (actor_user_id),
                    KEY idx_audit_created (created_at)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS password_reset_tokens (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    user_id BIGINT NOT NULL,
                    token_hash CHAR(64) NOT NULL,
                    expires_at DATETIME NOT NULL,
                    used_at DATETIME NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_pwd_reset_hash (token_hash),
                    KEY idx_pwd_reset_user (user_id),
                    KEY idx_pwd_reset_expires (expires_at)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS login_events (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    user_id BIGINT NULL,
                    login_identifier VARCHAR(255) NOT NULL,
                    login_type VARCHAR(32) NOT NULL,
                    result ENUM('SUCCESS','FAIL') NOT NULL,
                    reason VARCHAR(128) NULL,
                    ip VARCHAR(64) NULL,
                    ua VARCHAR(512) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_login_user_created (user_id, created_at),
                    KEY idx_login_result_created (result, created_at),
                    KEY idx_login_identifier_created (login_identifier, created_at)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS site_settings (
                    setting_key VARCHAR(64) PRIMARY KEY,
                    setting_value VARCHAR(255) NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT IGNORE INTO site_settings (setting_key, setting_value)
                VALUES ('client_require_login', '1')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT IGNORE INTO site_settings (setting_key, setting_value)
                VALUES ('client_allow_register', '1')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_usage_daily (
                    user_id BIGINT NOT NULL,
                    usage_date DATE NOT NULL,
                    api_requests INT NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, usage_date),
                    KEY idx_usage_date (usage_date)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_access_logs (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    user_id BIGINT NOT NULL,
                    username VARCHAR(64) NOT NULL,
                    role VARCHAR(32) NOT NULL,
                    path VARCHAR(1024) NOT NULL,
                    method VARCHAR(16) NOT NULL,
                    status_code INT NOT NULL,
                    ip VARCHAR(64) NULL,
                    user_agent VARCHAR(512) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_ual_user_id (user_id),
                    KEY idx_ual_created (created_at),
                    KEY idx_ual_user_created (user_id, created_at)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS strategy_configs (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    strategy_id VARCHAR(64) UNIQUE NOT NULL,
                    is_visible TINYINT(1) NOT NULL DEFAULT 1,
                    strategy_name VARCHAR(255) NOT NULL,
                    source VARCHAR(128) NOT NULL DEFAULT '',
                    remark TEXT NULL,
                    file_dir VARCHAR(500) NOT NULL DEFAULT '',
                    file_name VARCHAR(255) NOT NULL,
                    weight_display_mode ENUM('holding') NOT NULL DEFAULT 'holding',
                    benchmark_code VARCHAR(32) NOT NULL,
                    benchmark_name VARCHAR(128) NOT NULL,
                    strategy_intro TEXT NOT NULL,
                    status ENUM('enabled','disabled') NOT NULL DEFAULT 'enabled',
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
            )
        )
        # 历史库补字段：source / remark
        cfg_cols = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'strategy_configs'
                """
            )
        ).mappings().all()
        cfg_set = {str(r["COLUMN_NAME"]).lower() for r in cfg_cols}
        if "source" not in cfg_set:
            conn.execute(
                text("ALTER TABLE strategy_configs ADD COLUMN source VARCHAR(128) NOT NULL DEFAULT ''")
            )
        if "remark" not in cfg_set:
            conn.execute(text("ALTER TABLE strategy_configs ADD COLUMN remark TEXT NULL"))
        if "strategy_category" not in cfg_set:
            conn.execute(
                text(
                    "ALTER TABLE strategy_configs ADD COLUMN strategy_category VARCHAR(128) NOT NULL DEFAULT ''"
                )
            )
        if "rebalance_frequency" not in cfg_set:
            conn.execute(
                text(
                    "ALTER TABLE strategy_configs ADD COLUMN rebalance_frequency VARCHAR(128) NOT NULL DEFAULT ''"
                )
            )
        # 净值固定为持仓权重：历史 enum 纠偏后收紧为仅 holding（失败则下次启动重试）
        try:
            conn.execute(
                text(
                    "UPDATE strategy_configs SET weight_display_mode='holding' "
                    "WHERE weight_display_mode IN ('industry_neutral','both')"
                )
            )
        except Exception:
            pass
        try:
            conn.execute(
                text(
                    "ALTER TABLE strategy_configs MODIFY COLUMN weight_display_mode "
                    "ENUM('holding') NOT NULL DEFAULT 'holding'"
                )
            )
        except Exception:
            pass
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS strategy_positions (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    strategy_id VARCHAR(64) NOT NULL,
                    rebalance_date DATE NOT NULL,
                    stock_code VARCHAR(32) NOT NULL,
                    holding_weight DECIMAL(12,8) NULL,
                    industry_neutral_weight DECIMAL(12,8) NULL,
                    UNIQUE KEY uk_pos (strategy_id, rebalance_date, stock_code),
                    KEY idx_pos_date (strategy_id, rebalance_date)
                )
                """
            )
        )
        # 老库若仅有自增主键，按 strategy_id 聚合会全表扫；补 (strategy_id, rebalance_date) 索引
        try:
            pos_lead = conn.execute(
                text(
                    """
                    SELECT COUNT(*) AS c
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND table_name = 'strategy_positions'
                      AND seq_in_index = 1
                      AND column_name = 'strategy_id'
                    """
                )
            ).mappings().first()
            if pos_lead and int(pos_lead.get("c") or 0) == 0:
                conn.execute(
                    text(
                        "ALTER TABLE strategy_positions "
                        "ADD INDEX idx_pos_date (strategy_id, rebalance_date)"
                    )
                )
        except Exception:
            pass
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS strategy_holding_daily (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    strategy_id VARCHAR(64) NOT NULL,
                    trade_date DATE NOT NULL,
                    rebalance_date DATE NOT NULL,
                    stock_code VARCHAR(32) NOT NULL,
                    stock_name VARCHAR(128) NULL,
                    period_weight DECIMAL(12,8) NULL,
                    latest_weight DECIMAL(12,8) NULL,
                    latest_price DECIMAL(18,6) NULL,
                    last_1d_pct DECIMAL(12,6) NULL,
                    period_return DECIMAL(12,6) NULL,
                    ret_5d DECIMAL(12,6) NULL,
                    ret_20d DECIMAL(12,6) NULL,
                    ret_60d DECIMAL(12,6) NULL,
                    ret_ytd DECIMAL(12,6) NULL,
                    market_cap DECIMAL(22,4) NULL,
                    industry_name VARCHAR(128) NULL,
                    pe DECIMAL(18,6) NULL,
                    pb DECIMAL(18,6) NULL,
                    UNIQUE KEY uk_daily (strategy_id, trade_date, rebalance_date, stock_code),
                    KEY idx_daily (strategy_id, trade_date),
                    KEY idx_daily_rb (strategy_id, rebalance_date)
                )
                """
            )
        )
        # 旧库唯一键不含 rebalance_date，多期调仓会互相覆盖，需迁移一次
        row = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'strategy_holding_daily'
                  AND index_name = 'uk_daily'
                  AND non_unique = 0
                """
            )
        ).mappings().first()
        uk_cols = int(row["c"] or 0) if row else 0
        if uk_cols == 3:
            conn.execute(text("ALTER TABLE strategy_holding_daily DROP INDEX uk_daily"))
            conn.execute(
                text(
                    """
                    ALTER TABLE strategy_holding_daily
                    ADD UNIQUE KEY uk_daily (strategy_id, trade_date, rebalance_date, stock_code)
                """
            )
        )
        hd_cols = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'strategy_holding_daily'
                """
            )
        ).mappings().all()
        hd_set = {str(r.get("COLUMN_NAME") or r.get("column_name") or "").lower() for r in hd_cols}
        if "ret_10d" in hd_set and "ret_5d" not in hd_set:
            conn.execute(
                text(
                    "ALTER TABLE strategy_holding_daily CHANGE ret_10d ret_5d DECIMAL(12,6) NULL"
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_strategy_follows (
                    username VARCHAR(64) NOT NULL,
                    strategy_id VARCHAR(64) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (username, strategy_id),
                    KEY idx_follow_strategy (strategy_id)
                )
                """
            )
        )
        conn.execute(text("DROP TABLE IF EXISTS stock_ai_insights"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS strategy_update_jobs (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    job_type ENUM('SCHEDULED','MANUAL') NOT NULL,
                    status ENUM('RUNNING','SUCCESS','FAILED') NOT NULL,
                    triggered_by VARCHAR(64) NOT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    message TEXT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS admin_sync_jobs (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    status ENUM('QUEUED','RUNNING','SUCCESS','FAILED') NOT NULL DEFAULT 'QUEUED',
                    stage VARCHAR(64) NULL,
                    message TEXT NULL,
                    strategy_ids_json JSON NOT NULL,
                    import_mode VARCHAR(16) NOT NULL,
                    triggered_by VARCHAR(64) NOT NULL,
                    result_json JSON NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at DATETIME NULL,
                    finished_at DATETIME NULL,
                    KEY idx_admin_sync_status (status),
                    KEY idx_admin_sync_created (created_at)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS strategy_nav_daily (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    strategy_id VARCHAR(64) NOT NULL,
                    trade_date DATE NOT NULL,
                    nav_unit DECIMAL(18,8) NOT NULL,
                    daily_ret DECIMAL(12,6) NULL,
                    benchmark_ret DECIMAL(12,6) NULL,
                    benchmark_nav DECIMAL(18,8) NULL,
                    rebalance_date DATE NULL,
                    source_job_id BIGINT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_nav (strategy_id, trade_date),
                    KEY idx_nav_date (strategy_id, trade_date)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS data_import_definitions (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    code VARCHAR(64) NOT NULL,
                    display_name VARCHAR(255) NOT NULL,
                    default_file_path VARCHAR(1024) NULL,
                    description TEXT NULL,
                    meta_json JSON NULL,
                    enabled TINYINT(1) NOT NULL DEFAULT 1,
                    sort_order INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_data_import_def_code (code)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS data_import_batches (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    definition_code VARCHAR(64) NOT NULL,
                    source_file_path VARCHAR(1024) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    rows_ok INT NOT NULL DEFAULT 0,
                    rows_fail INT NOT NULL DEFAULT 0,
                    message TEXT NULL,
                    actor_user_id BIGINT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    KEY idx_data_import_batches_code (definition_code),
                    KEY idx_data_import_batches_created (created_at)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS supplement_company_profiles (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    definition_code VARCHAR(64) NOT NULL,
                    stock_code VARCHAR(512) NOT NULL,
                    last_batch_id BIGINT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_supplement_company (definition_code, stock_code),
                    KEY idx_supplement_company_stock (stock_code(64))
                )
                """
            )
        )
        _sc_len = conn.execute(
            text(
                """
                SELECT CHARACTER_MAXIMUM_LENGTH AS m
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'supplement_company_profiles'
                  AND COLUMN_NAME = 'stock_code'
                """
            )
        ).scalar()
        if _sc_len is not None and int(_sc_len) < 512:
            conn.execute(
                text(
                    """
                    ALTER TABLE supplement_company_profiles
                    MODIFY COLUMN stock_code VARCHAR(512) NOT NULL
                    """
                )
            )
        _has_row_data = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'supplement_company_profiles'
                  AND COLUMN_NAME = 'row_data'
                """
            )
        ).scalar()
        if int(_has_row_data or 0) > 0:
            conn.execute(text("ALTER TABLE supplement_company_profiles DROP COLUMN row_data"))
        _def_path = (settings.supplement_company_excel_path or "").strip() or str(
            Path(settings.strategy_root_dir) / "数据" / "公司资料.xlsx"
        )
        _meta_seed = json.dumps(
            {"unique_source_column": "stock_code", "unique_source_columns": []},
            ensure_ascii=False,
        )
        conn.execute(
            text(
                """
                INSERT IGNORE INTO data_import_definitions
                (code, display_name, default_file_path, description, enabled, sort_order, meta_json)
                VALUES
                (
                    'company_profile_excel',
                    '补充数据（Excel/CSV）',
                    :p,
                    '从 Excel/CSV 导入补充数据；表头与库字段一一对应，缺列自动 ALTER 补充。',
                    1,
                    10,
                    CAST(:meta AS JSON)
                )
                """
            ),
            {"p": _def_path[:1024], "meta": _meta_seed},
        )
        conn.execute(
            text(
                """
                UPDATE data_import_definitions
                SET meta_json = CAST(:meta AS JSON)
                WHERE code = 'company_profile_excel' AND meta_json IS NULL
                """
            ),
            {"meta": _meta_seed},
        )
        for _old_def in (
            r"D:\mysql\展示策略\数据\公司资料.xlsx",
            "D:/mysql/展示策略/数据/公司资料.xlsx",
        ):
            conn.execute(
                text(
                    """
                    UPDATE data_import_definitions
                    SET default_file_path=:newp,
                        meta_json=CAST(:meta AS JSON)
                    WHERE code='company_profile_excel' AND default_file_path=:oldp
                    """
                ),
                {"newp": _def_path[:1024], "meta": _meta_seed, "oldp": _old_def[:1024]},
            )
        conn.execute(
            text(
                """
                UPDATE data_import_definitions
                SET display_name = '补充数据（Excel/CSV）',
                    description = '从 Excel/CSV 导入补充数据；表头与库字段一一对应；唯一键列在 meta_json（unique_source_column / unique_source_columns）或导入请求中配置；新表头首次导入时自动 ADD COLUMN。'
                WHERE code = 'company_profile_excel'
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT IGNORE INTO users
                (username, password, role, org_id, status, password_is_system_generated, password_changed_at)
                VALUES
                ('admin','admin123','admin','org-a','active',0,NOW()),
                ('editor','editor123','editor','org-a','active',0,NOW()),
                ('viewer','viewer123','viewer','org-b','active',0,NOW())
                """
            )
        )
        conn.commit()

    app_engine = create_engine(
        DB_URL,
        pool_pre_ping=True,
        pool_size=max(1, int(settings.db_pool_size)),
        max_overflow=max(0, int(settings.db_max_overflow)),
        pool_timeout=max(5, int(settings.db_pool_timeout)),
        pool_recycle=max(30, int(settings.db_pool_recycle)),
    )
    global SessionLocalFactory
    SessionLocalFactory = sessionmaker(bind=app_engine, autoflush=False, autocommit=False)


SessionLocalFactory = None


def get_session():
    if SessionLocalFactory is None:
        raise RuntimeError("Database not initialized")
    db = SessionLocalFactory()
    try:
        yield db
    finally:
        db.close()