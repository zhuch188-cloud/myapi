"""Wind 行情：使用远程 SQL Server（WindDB）。未配置 WIND_SQLSERVER_SERVER 时跳过初始化，依赖 Wind 的任务会失败或需稍后配置。"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.orm import Session

from app.config import settings

logger = logging.getLogger(__name__)

_wind_engine: Engine | None = None
_wind_ready: bool = False
_wind_last_error: str | None = None


def _parse_sqlserver_host_port(server_raw: str, default_port: int) -> tuple[str, int]:
    """
    支持两种写法：
    - host + 独立端口：server_raw 不含「,数字端口」时用 default_port；
    - host,port：与 pyodbc 常见写法一致，如 111.229.130.90,16555（右侧为整数则视为端口）。
    """
    s = (server_raw or "").strip()
    if not s:
        return "", int(default_port or 1433)
    if "," in s:
        left, right = s.rsplit(",", 1)
        rs = right.strip()
        if rs.isdigit():
            return left.strip(), int(rs)
    return s, int(default_port or 1433)


def _normalize_odbc_driver(driver_raw: str) -> str:
    d = (driver_raw or "").strip()
    if len(d) >= 2 and d[0] == "{" and d[-1] == "}":
        return d[1:-1].strip()
    return d


def wind_status() -> dict[str, Any]:
    """供 /health 展示：含解析后的 host/port 与最近一次连接错误。"""
    server_raw = (settings.wind_sqlserver_server or "").strip()
    host, port = _parse_sqlserver_host_port(
        server_raw, int(settings.wind_sqlserver_port or 1433)
    )
    drivers: list[str] = []
    try:
        import pyodbc

        drivers = list(pyodbc.drivers())
    except Exception as ex:
        drivers = [f"(pyodbc.drivers failed: {ex})"]
    return {
        "configured": bool(server_raw),
        "ready": use_remote_sqlserver(),
        "host": host,
        "port": port,
        "database": (settings.wind_sqlserver_database or "winddb").strip(),
        "driver": _normalize_odbc_driver(
            settings.wind_sqlserver_driver or "ODBC Driver 17 for SQL Server"
        ),
        "odbc_drivers_installed": drivers,
        "last_error": _wind_last_error,
    }


def init_wind_backend() -> str:
    """应用启动时调用：已配置服务器则连接 SQL Server；未配置则跳过（便于本地仅连 MySQL）。"""
    global _wind_engine, _wind_ready, _wind_last_error
    _wind_last_error = None
    if _wind_ready and _wind_engine is not None:
        return "mssql"
    server_raw = (settings.wind_sqlserver_server or "").strip()
    if not server_raw:
        _wind_engine = None
        _wind_ready = False
        logger.warning(
            "Wind: 未设置 WIND_SQLSERVER_SERVER，已跳过 SQL Server 初始化；"
            "「立即更新」/导入后净值等依赖 Wind 的功能不可用，直至在 .env 中配置并重启。"
        )
        return "disabled"
    host, port = _parse_sqlserver_host_port(
        server_raw, int(settings.wind_sqlserver_port or 1433)
    )
    driver = _normalize_odbc_driver(
        settings.wind_sqlserver_driver or "ODBC Driver 17 for SQL Server"
    )
    try:
        eng = create_engine(
            URL.create(
                "mssql+pyodbc",
                username=settings.wind_sqlserver_user,
                password=settings.wind_sqlserver_password,
                host=host,
                port=port,
                database=(settings.wind_sqlserver_database or "winddb").strip(),
                query={
                    "driver": driver,
                    "TrustServerCertificate": "yes",
                },
            ),
            pool_pre_ping=True,
            pool_size=3,
            max_overflow=5,
            # 避免长任务后复用到已被 Wind 端关闭的空闲连接
            pool_recycle=180,
        )
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        _wind_engine = eng
        _wind_ready = True
        logger.info(
            "Wind: 已连接远程 SQL Server %s:%s / %s",
            host,
            port,
            (settings.wind_sqlserver_database or "winddb").strip(),
        )
    except Exception as e:
        _wind_engine = None
        _wind_ready = False
        _wind_last_error = str(e)
        logger.warning(
            "Wind: SQL Server 连接失败（%s:%s），已跳过；原因: %s",
            host,
            port,
            e,
        )
        return "disabled"
    return "mssql"


def use_remote_sqlserver() -> bool:
    """兼容旧调用：当前等价于「Wind 已用 SQL Server 初始化成功」。"""
    return _wind_ready and _wind_engine is not None


def _tbl(name: str) -> str:
    return f"dbo.{name}"


def get_wind_engine() -> Engine:
    if not _wind_ready or _wind_engine is None:
        raise RuntimeError("Wind SQL Server 未初始化，请先调用 init_wind_backend()")
    return _wind_engine


def open_wind(mysql_db: Session) -> Connection:
    """返回 SQL Server Connection（mysql_db 仅保留签名兼容，不再用于 Wind 查询）。"""
    _ = mysql_db
    return get_wind_engine().connect()


def close_wind(wind: Any, mysql_db: Session) -> None:
    _ = mysql_db
    if wind is None:
        return
    if hasattr(wind, "close"):
        wind.close()


def close_wind_safe(
    wind: Any,
    mysql_db: Session | None = None,
    *,
    timeout_sec: float = 8.0,
) -> None:
    """关闭 Wind 连接；超时则放弃等待，避免 SQL Server 僵连导致任务永久 RUNNING。"""
    if wind is None:
        return
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

    def _do_close() -> None:
        close_wind(wind, mysql_db)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(_do_close)
            fut.result(timeout=max(1.0, float(timeout_sec)))
    except FuturesTimeout:
        logger.warning(
            "Wind close timed out after %.0fs (connection may be stale); task continues",
            timeout_sec,
        )
    except Exception as ex:
        logger.warning("Wind close failed: %s", ex)


def sql_max_trade_dt() -> str:
    t = _tbl("ashareeodprices")
    return f"SELECT MAX(TRADE_DT) AS d FROM {t}"


def sql_trade_dates_range() -> str:
    t = _tbl("ashareeodprices")
    return f"""
        SELECT DISTINCT TRADE_DT AS d
        FROM {t}
        WHERE TRADE_DT >= :st AND TRADE_DT <= :ed
        ORDER BY TRADE_DT ASC
        """


def sql_trade_calendar_range() -> str:
    t = _tbl("asharecalendar")
    return f"""
        SELECT DISTINCT TRADE_DAYS AS d
        FROM {t}
        WHERE TRADE_DAYS >= :st AND TRADE_DAYS <= :ed
          AND S_INFO_EXCHMARKET IN ('SSE', 'SZSE')
        ORDER BY TRADE_DAYS ASC
        """


def sql_bulk_eod_range(quoted_codes: str) -> str:
    """
    一次拉取多只股票在 [st, td] 内的日频行情。
    表 dbo.AShareEODPrices（Wind《中国A股日行情》数据字典）：S_DQ_ADJCLOSE 为「复权收盘价」
    （字典释义为后复权收盘；缺失时回退 S_DQ_CLOSE），S_DQ_CLOSE 为不复权收盘价。
    日收益在应用侧用「上一交易日复权收盘」作昨收，与不复权 S_DQ_PRECLOSE 解耦。
    与 run_update / rebuild_nav_series / 个股走势图为同一数据源。
    """
    t = _tbl("ashareeodprices")
    return f"""
        SELECT
          S_INFO_WINDCODE AS c,
          TRADE_DT AS d,
          CAST(S_DQ_CLOSE AS FLOAT) AS raw_cl,
          COALESCE(
            TRY_CAST(S_DQ_ADJCLOSE AS FLOAT),
            CAST(S_DQ_CLOSE AS FLOAT)
          ) AS adj_cl
        FROM {t}
        WHERE TRADE_DT >= :st AND TRADE_DT <= :td
          AND S_INFO_WINDCODE IN ({quoted_codes})
        ORDER BY S_INFO_WINDCODE, TRADE_DT ASC
        """


def sql_bulk_index_eod_range(quoted_codes: str) -> str:
    """
    一次拉取多只指数在 [st, td] 内的日频行情。

    多数 Wind 库里 dbo.AIndexEODPrices **没有** S_DQ_ADJCLOSE（与 AShareEODPrices 字段集不同），
    仅有不复权收盘等字段；此处 raw_cl / adj_cl 均取 S_DQ_CLOSE，日收益仍为相邻交易日收盘比。
    """
    t = _tbl("aindexeodprices")
    return f"""
        SELECT
          S_INFO_WINDCODE AS c,
          TRADE_DT AS d,
          CAST(S_DQ_CLOSE AS FLOAT) AS raw_cl,
          CAST(S_DQ_CLOSE AS FLOAT) AS adj_cl
        FROM {t}
        WHERE TRADE_DT >= :st AND TRADE_DT <= :td
          AND S_INFO_WINDCODE IN ({quoted_codes})
        ORDER BY S_INFO_WINDCODE, TRADE_DT ASC
        """


def sql_quote_batch(quoted_codes: str) -> str:
    """日行情 + 名称；估值取最近一条衍生表；行业按申万一级(2021)映射。"""
    pr, de, di, swn_t, indc = (
        _tbl("ashareeodprices"),
        _tbl("asharedescription"),
        _tbl("ashareeodderivativeindicator"),
        _tbl("ashareswnindustriesclass"),
        _tbl("ashareindustriescode"),
    )
    return f"""
        SELECT
          pr.S_INFO_WINDCODE AS stock_code,
          pr.S_DQ_CLOSE AS latest_price,
          pr.S_DQ_PRECLOSE AS prev_close,
          de.S_INFO_NAME AS stock_name,
          di.S_VAL_MV AS market_cap,
          COALESCE(di.S_VAL_PE_TTM, di.S_VAL_PE) AS pe,
          di.S_VAL_PB_NEW AS pb,
          ind_l1.INDUSTRIESNAME AS industry_name
        FROM {pr} pr
        JOIN (
          SELECT
            S_INFO_WINDCODE,
            MAX(TRADE_DT) AS latest_dt
          FROM {pr}
          WHERE TRADE_DT <= :td_compact
            AND S_INFO_WINDCODE IN ({quoted_codes})
          GROUP BY S_INFO_WINDCODE
        ) m
          ON m.S_INFO_WINDCODE = pr.S_INFO_WINDCODE
          AND m.latest_dt = pr.TRADE_DT
        LEFT JOIN {de} de
          ON de.S_INFO_WINDCODE = pr.S_INFO_WINDCODE
        OUTER APPLY (
          SELECT TOP 1
            di2.S_VAL_MV,
            di2.S_VAL_PE_TTM,
            di2.S_VAL_PE,
            di2.S_VAL_PB_NEW
          FROM {di} di2
          WHERE di2.S_INFO_WINDCODE = pr.S_INFO_WINDCODE
            AND di2.TRADE_DT <= pr.TRADE_DT
          ORDER BY di2.TRADE_DT DESC
        ) di
        OUTER APPLY (
          SELECT TOP 1 swc.SW_IND_CODE
          FROM {swn_t} swc
          WHERE swc.S_INFO_WINDCODE = pr.S_INFO_WINDCODE
            AND swc.ENTRY_DT <= pr.TRADE_DT
            AND (swc.REMOVE_DT IS NULL OR swc.REMOVE_DT = '' OR swc.REMOVE_DT > pr.TRADE_DT)
          ORDER BY swc.ENTRY_DT DESC
        ) sw
        LEFT JOIN {indc} ind_l1
          ON sw.SW_IND_CODE IS NOT NULL
          AND ind_l1.LEVELNUM = 2
          AND ind_l1.USED = 1
          AND (
            ind_l1.INDUSTRIESCODE = LEFT(sw.SW_IND_CODE, 4) + REPLICATE('0', 12)
            OR ind_l1.NEW_INDUSTRIESCODE = LEFT(sw.SW_IND_CODE, 4) + REPLICATE('0', 12)
          )
        """


def sql_history_desc() -> str:
    t = _tbl("ashareeodprices")
    return f"""
        SELECT TOP (260) TRADE_DT, S_DQ_CLOSE
        FROM {t}
        WHERE S_INFO_WINDCODE = :scode
          AND TRADE_DT <= :td_compact
        ORDER BY TRADE_DT DESC
        """


def sql_ytd_first() -> str:
    t = _tbl("ashareeodprices")
    return f"""
        SELECT TOP (1) S_DQ_CLOSE
        FROM {t}
        WHERE S_INFO_WINDCODE = :scode
          AND TRADE_DT >= :ytd_start
          AND TRADE_DT <= :td_compact
        ORDER BY TRADE_DT ASC
        """


def sql_period_first() -> str:
    t = _tbl("ashareeodprices")
    return f"""
        SELECT TOP (1) S_DQ_CLOSE
        FROM {t}
        WHERE S_INFO_WINDCODE = :scode
          AND TRADE_DT >= :rebalance_start
          AND TRADE_DT <= :td_compact
        ORDER BY TRADE_DT ASC
        """
