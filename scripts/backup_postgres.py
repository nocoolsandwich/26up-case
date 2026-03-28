from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Iterable

try:
    from scripts.project_config import PROJECT_ROOT, load_project_config
except ModuleNotFoundError:  # 兼容 python scripts/backup_postgres.py 直接运行
    from project_config import PROJECT_ROOT, load_project_config


DEFAULT_DATABASES = ("event_news", "event_quant")
DEFAULT_PG_DUMP_BIN = "/opt/homebrew/opt/postgresql@16/bin/pg_dump"
DEFAULT_BACKUP_ROOT = PROJECT_ROOT / "data" / "db_backups"
DEFAULT_CONTAINER_NAME = "event-news-pg"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = "postgres"


def build_backup_dir(timestamp: str, backup_root: Path = DEFAULT_BACKUP_ROOT) -> Path:
    return backup_root / timestamp


def build_docker_pg_dump_command(
    container_name: str,
    db_name: str,
    db_user: str = DEFAULT_DB_USER,
    db_password: str = DEFAULT_DB_PASSWORD,
) -> list[str]:
    return [
        "docker",
        "exec",
        "-e",
        f"PGPASSWORD={db_password}",
        container_name,
        "pg_dump",
        "-Fc",
        "-U",
        db_user,
        "-d",
        db_name,
    ]


def build_local_pg_dump_command(
    pg_dump_bin: str,
    db_name: str,
    output_path: Path,
) -> list[str]:
    return [pg_dump_bin, "-Fc", "-d", db_name, "-f", str(output_path)]


def write_backup_manifest(backup_dir: Path, backups: Iterable[dict]) -> Path:
    payload = {
        "backup_dir": str(backup_dir),
        "generated_at": datetime.now().isoformat(),
        "backups": list(backups),
    }
    manifest_path = backup_dir / "manifest.json"
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return manifest_path


def run_backup(
    backup_dir: Path,
    databases: Iterable[str],
    mode: str = "local",
    pg_dump_bin: str = DEFAULT_PG_DUMP_BIN,
    container_name: str = DEFAULT_CONTAINER_NAME,
    db_user: str = DEFAULT_DB_USER,
    db_password: str = DEFAULT_DB_PASSWORD,
) -> list[dict]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []
    for db_name in databases:
        file_name = f"{db_name}.dump"
        output_path = backup_dir / file_name
        if mode == "docker":
            command = build_docker_pg_dump_command(
                container_name=container_name,
                db_name=db_name,
                db_user=db_user,
                db_password=db_password,
            )
            with output_path.open("wb") as fp:
                subprocess.run(command, check=True, stdout=fp)
        elif mode == "local":
            command = build_local_pg_dump_command(
                pg_dump_bin=pg_dump_bin,
                db_name=db_name,
                output_path=output_path,
            )
            subprocess.run(command, check=True)
        else:
            raise ValueError(f"不支持的备份模式: {mode}")
        results.append(
            {
                "db_name": db_name,
                "file_name": file_name,
                "bytes": output_path.stat().st_size,
            }
        )
    return results


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="备份项目 PostgreSQL 数据库到项目目录")
    parser.add_argument("--backup-root", default=None, help="备份根目录")
    parser.add_argument("--timestamp", default=datetime.now().strftime("%Y%m%d_%H%M%S"), help="备份目录时间戳")
    parser.add_argument("--mode", choices=["local", "docker"], default=None, help="备份模式")
    parser.add_argument("--pg-dump-bin", default=None, help="本机 pg_dump 路径")
    parser.add_argument("--container-name", default=None, help="PostgreSQL 容器名")
    parser.add_argument("--db-user", default=None, help="数据库用户")
    parser.add_argument("--db-password", default=None, help="数据库密码")
    parser.add_argument("--db", action="append", dest="databases", default=[], help="要备份的数据库，可重复传入")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    config = load_project_config(PROJECT_ROOT)
    backup_root = Path(args.backup_root or config["paths"]["backup_root"])
    if not backup_root.is_absolute():
        backup_root = PROJECT_ROOT / backup_root
    backup_dir = build_backup_dir(args.timestamp, backup_root=backup_root)
    databases = tuple(args.databases) if args.databases else DEFAULT_DATABASES
    mode = args.mode or config["backup"]["mode"]
    pg_dump_bin = args.pg_dump_bin or config["backup"].get("pg_dump_bin", DEFAULT_PG_DUMP_BIN)
    docker_config = config["backup"]["docker"]
    container_name = args.container_name or docker_config["container_name"]
    db_user = args.db_user or docker_config.get("db_user", DEFAULT_DB_USER)
    db_password = args.db_password or docker_config.get("db_password", DEFAULT_DB_PASSWORD)
    backups = run_backup(
        backup_dir=backup_dir,
        databases=databases,
        mode=mode,
        pg_dump_bin=pg_dump_bin,
        container_name=container_name,
        db_user=db_user,
        db_password=db_password,
    )
    manifest_path = write_backup_manifest(backup_dir, backups)
    print(
        json.dumps(
            {
                "backup_dir": str(backup_dir),
                "container_name": args.container_name,
                "databases": list(databases),
                "manifest": str(manifest_path),
                "backups": backups,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
