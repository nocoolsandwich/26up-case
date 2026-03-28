from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "project.yaml"
REQUIRED_PATHS = [
    ("postgres", "event_news_dsn"),
    ("postgres", "event_quant_dsn"),
    ("tushare", "token"),
    ("tushare", "http_url"),
    ("paths", "stock_file"),
    ("paths", "marco_file"),
    ("paths", "data_dir"),
    ("paths", "backup_root"),
    ("backup", "mode"),
    ("backup", "docker", "container_name"),
    ("case", "sheet_name"),
]


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到项目配置文件: {path}")
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise ValueError("项目配置文件必须是对象结构")
    return payload


def _get_nested(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            raise ValueError(f"项目配置缺少字段: {'.'.join(keys)}")
        current = current[key]
    if current in (None, ""):
        raise ValueError(f"项目配置字段不能为空: {'.'.join(keys)}")
    return current


def load_project_config(project_root: str | Path | None = None) -> dict[str, Any]:
    root = Path(project_root) if project_root else PROJECT_ROOT
    config_path = root / "config" / "project.yaml"
    payload = _read_yaml(config_path)
    for path_keys in REQUIRED_PATHS:
        _get_nested(payload, tuple(path_keys))
    return payload
