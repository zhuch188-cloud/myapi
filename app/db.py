import json
from pathlib import Path

import libsql
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import settings
from app.sql_dialect import coerce_bind_parameters, sql_now

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _LibsqlCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, statement, parameters=None):
        return self._cursor.execute(statement, coerce_bind_parameters(parameters))

    def executemany(self, statement, parameters):
        seq = parameters or []
        return self._cursor.executemany(
            statement, [coerce_bind_parameters(p) for p in seq]
        )

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _LibsqlDbapiAdapter:
    """包装 libsql 连接，满足 SQLAlchemy sqlite 方言对 create_function 的调用。"""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _LibsqlCursor(self._conn.cursor())

    def execute(self, statement, parameters=None):
        return self._conn.execute(statement, coerce_bind_parameters(parameters))

    def executemany(self, statement, parameters):
        seq = parameters or []
        return self._conn.executemany(
            statement, [coerce_bind_parameters(p) for p in seq]
        )

    def create_function(self, name, num_params, func, deterministic=False):
        return None

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _libsql_connect():
    remote = (settings.turso_database_url or "").strip()
    token = (settings.turso_auth_token or "").strip()
    if not remote:
        raise RuntimeError("TURSO_DATABASE_URL is required")
    if not token:
        raise RuntimeError("TURSO_AUTH_TOKEN is required")

    replica = (settings.turso_local_replica or "").strip()
    if replica:
        path = Path(replica)
        if not path.is_absolute():
            path = _PROJECT_ROOT / path
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = libsql.connect(
            f"file:{path.as_posix()}",
            sync_url=remote,
            auth_token=token,
            _check_same_thread=False,
        )
    else:
        conn = libsql.connect(remote, auth_token=token, _check_same_thread=False)
    return _LibsqlDbapiAdapter(conn)


def create_app_engine():
    eng = create_engine(
        "sqlite://",
        creator=_libsql_connect,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(eng, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    @event.listens_for(eng, "before_cursor_execute", retval=True)
    def _coerce_cursor_params(_conn, _cursor, statement, parameters, _context, _executemany):
        if parameters is not None:
            return statement, coerce_bind_parameters(parameters)
        return statement, parameters

    return eng


def _sqlite_table_columns(conn, table: str) -> set[str]:
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return {str(r[1]) for r in rows}


def _sqlite_add_column_if_missing(conn, table: str, column: str, coldef: str) -> None:
    if column not in _sqlite_table_columns(conn, table):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}"))


def _apply_runtime_schema_migrations(conn) -> None:
    """已有库增量加列/新表（CREATE IF NOT EXISTS 不会补列）。"""
    _sqlite_add_column_if_missing(conn, "data_import_batches", "resume_from_row", "INTEGER NOT NULL DEFAULT 0")
    _sqlite_add_column_if_missing(conn, "data_import_batches", "rows_total", "INTEGER NULL")
    _sqlite_add_column_if_missing(conn, "data_import_batches", "checkpoint_json", "TEXT NULL")
    _sqlite_add_column_if_missing(conn, "data_import_batches", "progress_at", "TEXT NULL")
    _sqlite_add_column_if_missing(conn, "admin_sync_jobs", "checkpoint_json", "TEXT NULL")
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS strategy_import_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL DEFAULT 'QUEUED',
                import_mode TEXT NOT NULL,
                strategy_ids_json TEXT NOT NULL,
                completed_strategy_ids_json TEXT NOT NULL DEFAULT '[]',
                imported_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                errors_json TEXT NULL,
                message TEXT NULL,
                triggered_by TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                started_at TEXT NULL,
                finished_at TEXT NULL
            )
            """
        )
    )
    conn.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_strategy_import_status ON strategy_import_jobs (status)"
        )
    )
    conn.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_strategy_import_created ON strategy_import_jobs (created_at)"
        )
    )


def init_database() -> None:
    engine = create_app_engine()
    with engine.connect() as conn:
        for stmt in _SCHEMA_STATEMENTS:
            conn.execute(text(stmt))
        _apply_runtime_schema_migrations(conn)
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO site_settings (setting_key, setting_value)
                VALUES ('client_require_login', '1')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO site_settings (setting_key, setting_value)
                VALUES ('client_allow_register', '1')
                """
            )
        )
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
                INSERT OR IGNORE INTO data_import_definitions
                (code, display_name, default_file_path, description, enabled, sort_order, meta_json)
                VALUES
                (
                    'company_profile_excel',
                    '补充数据（Excel/CSV）',
                    :p,
                    '从 Excel/CSV 导入补充数据；表头与库字段一一对应，缺列自动 ALTER 补充。',
                    1,
                    10,
                    :meta
                )
                """
            ),
            {"p": _def_path[:1024], "meta": _meta_seed},
        )
        conn.execute(
            text(
                """
                UPDATE data_import_definitions
                SET meta_json = :meta
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
                        meta_json=:meta
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
                f"""
                INSERT OR IGNORE INTO users
                (username, password, role, org_id, status, password_is_system_generated, password_changed_at)
                VALUES
                ('admin','admin123','admin','org-a','active',0,{sql_now()}),
                ('editor','editor123','editor','org-a','active',0,{sql_now()}),
                ('viewer','viewer123','viewer','org-b','active',0,{sql_now()})
                """
            )
        )
        conn.commit()

    global SessionLocalFactory
    SessionLocalFactory = sessionmaker(bind=engine, autoflush=False, autocommit=False)


_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        role TEXT NOT NULL CHECK (role IN ('admin','editor','viewer')),
        org_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled','locked')),
        password_is_system_generated INTEGER NOT NULL DEFAULT 1,
        password_changed_at TEXT NULL,
        failed_login_count INTEGER NOT NULL DEFAULT 0,
        locked_until TEXT NULL,
        last_login_at TEXT NULL,
        last_login_ip TEXT NULL,
        nickname TEXT NULL,
        contact_phone TEXT NULL,
        contact_email TEXT NULL,
        profile_bio TEXT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uk_users_nickname ON users (nickname)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uk_users_contact_phone ON users (contact_phone)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uk_users_contact_email ON users (contact_email)",
    """
    CREATE TABLE IF NOT EXISTS user_devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        device_token_hash TEXT NOT NULL,
        device_token_expires_at TEXT NOT NULL,
        trusted INTEGER NOT NULL DEFAULT 1,
        device_name TEXT NULL,
        ua TEXT NULL,
        platform TEXT NULL,
        ip_first TEXT NULL,
        ip_last TEXT NULL,
        last_seen_at TEXT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        revoked_at TEXT NULL
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uk_device_token_hash ON user_devices (device_token_hash)",
    "CREATE INDEX IF NOT EXISTS idx_user_devices_user ON user_devices (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_user_devices_expires ON user_devices (device_token_expires_at)",
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor_user_id INTEGER NULL,
        action TEXT NOT NULL,
        target_user_id INTEGER NULL,
        detail_json TEXT NULL,
        ip TEXT NULL,
        ua TEXT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_audit_target ON audit_logs (target_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs (actor_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs (created_at)",
    """
    CREATE TABLE IF NOT EXISTS password_reset_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token_hash TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        used_at TEXT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pwd_reset_hash ON password_reset_tokens (token_hash)",
    "CREATE INDEX IF NOT EXISTS idx_pwd_reset_user ON password_reset_tokens (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_pwd_reset_expires ON password_reset_tokens (expires_at)",
    """
    CREATE TABLE IF NOT EXISTS login_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NULL,
        login_identifier TEXT NOT NULL,
        login_type TEXT NOT NULL,
        result TEXT NOT NULL CHECK (result IN ('SUCCESS','FAIL')),
        reason TEXT NULL,
        ip TEXT NULL,
        ua TEXT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_login_user_created ON login_events (user_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_login_result_created ON login_events (result, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_login_identifier_created ON login_events (login_identifier, created_at)",
    """
    CREATE TABLE IF NOT EXISTS site_settings (
        setting_key TEXT PRIMARY KEY,
        setting_value TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_usage_daily (
        user_id INTEGER NOT NULL,
        usage_date TEXT NOT NULL,
        api_requests INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (user_id, usage_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_usage_date ON user_usage_daily (usage_date)",
    """
    CREATE TABLE IF NOT EXISTS user_access_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        role TEXT NOT NULL,
        path TEXT NOT NULL,
        method TEXT NOT NULL,
        status_code INTEGER NOT NULL,
        ip TEXT NULL,
        user_agent TEXT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ual_user_id ON user_access_logs (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_ual_created ON user_access_logs (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_ual_user_created ON user_access_logs (user_id, created_at)",
    """
    CREATE TABLE IF NOT EXISTS strategy_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL UNIQUE,
        is_visible INTEGER NOT NULL DEFAULT 1,
        strategy_name TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '',
        remark TEXT NULL,
        file_dir TEXT NOT NULL DEFAULT '',
        file_name TEXT NOT NULL,
        weight_display_mode TEXT NOT NULL DEFAULT 'holding',
        benchmark_code TEXT NOT NULL,
        benchmark_name TEXT NOT NULL,
        strategy_intro TEXT NOT NULL,
        strategy_category TEXT NOT NULL DEFAULT '',
        rebalance_frequency TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'enabled' CHECK (status IN ('enabled','disabled')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        rebalance_date TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        holding_weight REAL NULL,
        industry_neutral_weight REAL NULL,
        UNIQUE (strategy_id, rebalance_date, stock_code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pos_date ON strategy_positions (strategy_id, rebalance_date)",
    """
    CREATE TABLE IF NOT EXISTS strategy_holding_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        rebalance_date TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        stock_name TEXT NULL,
        period_weight REAL NULL,
        latest_weight REAL NULL,
        latest_price REAL NULL,
        last_1d_pct REAL NULL,
        period_return REAL NULL,
        ret_5d REAL NULL,
        ret_20d REAL NULL,
        ret_60d REAL NULL,
        ret_ytd REAL NULL,
        market_cap REAL NULL,
        industry_name TEXT NULL,
        pe REAL NULL,
        pb REAL NULL,
        UNIQUE (strategy_id, trade_date, rebalance_date, stock_code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_daily ON strategy_holding_daily (strategy_id, trade_date)",
    "CREATE INDEX IF NOT EXISTS idx_daily_rb ON strategy_holding_daily (strategy_id, rebalance_date)",
    """
    CREATE TABLE IF NOT EXISTS user_strategy_follows (
        username TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (username, strategy_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_follow_strategy ON user_strategy_follows (strategy_id)",
    "DROP TABLE IF EXISTS stock_ai_insights",
    """
    CREATE TABLE IF NOT EXISTS strategy_update_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_type TEXT NOT NULL CHECK (job_type IN ('SCHEDULED','MANUAL')),
        status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
        triggered_by TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT NULL,
        message TEXT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS admin_sync_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED','RUNNING','SUCCESS','FAILED')),
        stage TEXT NULL,
        message TEXT NULL,
        strategy_ids_json TEXT NOT NULL,
        import_mode TEXT NOT NULL,
        triggered_by TEXT NOT NULL,
        result_json TEXT NULL,
        checkpoint_json TEXT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        started_at TEXT NULL,
        finished_at TEXT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_admin_sync_status ON admin_sync_jobs (status)",
    "CREATE INDEX IF NOT EXISTS idx_admin_sync_created ON admin_sync_jobs (created_at)",
    """
    CREATE TABLE IF NOT EXISTS strategy_nav_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        nav_unit REAL NOT NULL,
        daily_ret REAL NULL,
        benchmark_ret REAL NULL,
        benchmark_nav REAL NULL,
        rebalance_date TEXT NULL,
        source_job_id INTEGER NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE (strategy_id, trade_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_nav_date ON strategy_nav_daily (strategy_id, trade_date)",
    """
    CREATE TABLE IF NOT EXISTS data_import_definitions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL UNIQUE,
        display_name TEXT NOT NULL,
        default_file_path TEXT NULL,
        description TEXT NULL,
        meta_json TEXT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        sort_order INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS data_import_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        definition_code TEXT NOT NULL,
        source_file_path TEXT NOT NULL,
        status TEXT NOT NULL,
        rows_ok INTEGER NOT NULL DEFAULT 0,
        rows_fail INTEGER NOT NULL DEFAULT 0,
        rows_total INTEGER NULL,
        resume_from_row INTEGER NOT NULL DEFAULT 0,
        checkpoint_json TEXT NULL,
        message TEXT NULL,
        actor_user_id INTEGER NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_data_import_batches_code ON data_import_batches (definition_code)",
    "CREATE INDEX IF NOT EXISTS idx_data_import_batches_created ON data_import_batches (created_at)",
    """
    CREATE TABLE IF NOT EXISTS supplement_company_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        definition_code TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        last_batch_id INTEGER NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE (definition_code, stock_code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_supplement_company_stock ON supplement_company_profiles (stock_code)",
]


SessionLocalFactory = None


def get_session():
    if SessionLocalFactory is None:
        raise RuntimeError("Database not initialized")
    db = SessionLocalFactory()
    try:
        yield db
    finally:
        db.close()
