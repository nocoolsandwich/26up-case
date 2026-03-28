from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


SKILL_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = SKILL_ROOT / "stock-wave-attribution.yaml"
REQUIRED_PATHS = [
    ("postgres", "event_news_dsn"),
    ("postgres", "event_quant_dsn"),
    ("tushare", "token"),
    ("tushare", "http_url"),
    ("paths", "analysis_dir"),
    ("paths", "plot_dir"),
    ("paths", "cache_dir"),
    ("chatgpt", "node_bin"),
    ("chatgpt", "script_path"),
]
PATH_FIELDS = {
    ("paths", "analysis_dir"),
    ("paths", "plot_dir"),
    ("paths", "cache_dir"),
    ("chatgpt", "script_path"),
}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到 skill 配置文件: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("skill 配置文件必须是对象结构")
    return payload


def _get_nested(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            raise ValueError(f"skill 配置缺少字段: {'.'.join(keys)}")
        current = current[key]
    if current in (None, ""):
        raise ValueError(f"skill 配置字段不能为空: {'.'.join(keys)}")
    return current


def _set_nested(payload: dict[str, Any], keys: tuple[str, ...], value: Any) -> None:
    current = payload
    for key in keys[:-1]:
        current = current[key]
    current[keys[-1]] = value


def load_skill_config(config_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(config_path).expanduser().resolve() if config_path else DEFAULT_CONFIG_PATH
    payload = _read_yaml(path)
    for path_keys in REQUIRED_PATHS:
        value = _get_nested(payload, tuple(path_keys))
        if tuple(path_keys) in PATH_FIELDS:
            resolved = (path.parent / str(value)).resolve() if not Path(str(value)).is_absolute() else Path(str(value))
            _set_nested(payload, tuple(path_keys), str(resolved))
    payload["config_path"] = str(path)
    payload["skill_root"] = str(SKILL_ROOT)
    return payload

