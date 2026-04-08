from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
import yaml

DEFAULT_EVENT_NEWS_DSN = "postgresql://postgres:postgres@localhost:5432/event_news"
DEFAULT_EVENT_QUANT_DSN = "postgresql://postgres:postgres@localhost:5432/event_quant"
DEFAULT_CONFIG_RELATIVE_PATH = Path("skills/stock-wave-attribution/stock-wave-attribution.yaml")
REQUIRED_TABLES = {
    "event_news": ("event_metadata",),
    "event_quant": ("raw_stock_daily_qfq",),
}


def _resolve_workspace_root(workspace_root: str | Path | None) -> Path:
    if workspace_root is not None:
        return Path(workspace_root).expanduser().resolve()
    return Path(__file__).resolve().parents[1]


def _resolve_config_path(workspace_root: str | Path | None = None, config_path: str | Path | None = None) -> Path:
    if config_path is not None:
        return Path(config_path).expanduser().resolve()
    return (_resolve_workspace_root(workspace_root) / DEFAULT_CONFIG_RELATIVE_PATH).resolve()


def _read_datastore_dsns(config_path: Path) -> dict[str, str]:
    if not config_path.exists():
        return {
            "event_news_dsn": DEFAULT_EVENT_NEWS_DSN,
            "event_quant_dsn": DEFAULT_EVENT_QUANT_DSN,
        }
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    postgres = payload.get("postgres", {}) if isinstance(payload, dict) else {}
    event_news_dsn = postgres.get("event_news_dsn") if isinstance(postgres, dict) else None
    event_quant_dsn = postgres.get("event_quant_dsn") if isinstance(postgres, dict) else None
    return {
        "event_news_dsn": str(event_news_dsn or DEFAULT_EVENT_NEWS_DSN),
        "event_quant_dsn": str(event_quant_dsn or DEFAULT_EVENT_QUANT_DSN),
    }


def _redact_dsn(dsn: str) -> str:
    parsed = urlsplit(dsn)
    if "@" not in parsed.netloc:
        return dsn
    credentials, host = parsed.netloc.rsplit("@", 1)
    if ":" in credentials:
        username, _ = credentials.split(":", 1)
        credentials = f"{username}:***"
    redacted_netloc = f"{credentials}@{host}"
    return urlunsplit((parsed.scheme, redacted_netloc, parsed.path, parsed.query, parsed.fragment))


def _check_datastore(
    name: str,
    dsn: str,
    *,
    required_tables: tuple[str, ...],
    connect=psycopg.connect,
) -> dict[str, object]:
    started = time.perf_counter()
    required = {table: False for table in required_tables}
    try:
        with connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("select current_database(), current_user")
                current_database, current_user = cur.fetchone()
                for table_name in required_tables:
                    cur.execute("select to_regclass(%s)", (f"public.{table_name}",))
                    required[table_name] = bool(cur.fetchone()[0])
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        missing_tables = [table for table, exists in required.items() if not exists]
        error = f"缺少表: {', '.join(missing_tables)}" if missing_tables else ""
        return {
            "name": name,
            "ok": not missing_tables,
            "dsn": _redact_dsn(dsn),
            "current_database": str(current_database),
            "current_user": str(current_user),
            "latency_ms": latency_ms,
            "required_tables": required,
            "error": error,
        }
    except Exception as exc:  # noqa: BLE001
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "name": name,
            "ok": False,
            "dsn": _redact_dsn(dsn),
            "latency_ms": latency_ms,
            "required_tables": required,
            "error": str(exc),
        }


def collect_datastore_health(
    *,
    workspace_root: str | Path | None = None,
    config_path: str | Path | None = None,
    connect=psycopg.connect,
) -> dict[str, object]:
    resolved_config_path = _resolve_config_path(workspace_root=workspace_root, config_path=config_path)
    dsns = _read_datastore_dsns(resolved_config_path)
    datastores = [
        _check_datastore(
            "event_news",
            dsns["event_news_dsn"],
            required_tables=REQUIRED_TABLES["event_news"],
            connect=connect,
        ),
        _check_datastore(
            "event_quant",
            dsns["event_quant_dsn"],
            required_tables=REQUIRED_TABLES["event_quant"],
            connect=connect,
        ),
    ]
    failed = [item for item in datastores if not item["ok"]]
    if failed:
        summary = "; ".join(
            f"{item['name']} 连接或表校验失败: {item.get('error') or 'unknown error'}" for item in failed
        )
    else:
        summary = "event_news / event_quant 已就绪"
    return {
        "ok": not failed,
        "summary": summary,
        "config_path": str(resolved_config_path),
        "datastores": datastores,
    }
