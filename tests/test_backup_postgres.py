import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.backup_postgres import (
    DEFAULT_BACKUP_ROOT,
    build_backup_dir,
    build_docker_pg_dump_command,
    build_local_pg_dump_command,
    write_backup_manifest,
)


class BackupPostgresTest(unittest.TestCase):
    def test_build_local_pg_dump_command_uses_local_binary_and_file_output(self):
        command = build_local_pg_dump_command(
            pg_dump_bin="/opt/homebrew/opt/postgresql@16/bin/pg_dump",
            db_name="event_quant",
            output_path=Path("/tmp/event_quant.dump"),
        )

        self.assertEqual(
            command,
            [
                "/opt/homebrew/opt/postgresql@16/bin/pg_dump",
                "-Fc",
                "-d",
                "event_quant",
                "-f",
                "/tmp/event_quant.dump",
            ],
        )

    def test_build_backup_dir_uses_project_backup_root(self):
        backup_dir = build_backup_dir("20260318_101500", backup_root=DEFAULT_BACKUP_ROOT)

        self.assertEqual(
            backup_dir,
            DEFAULT_BACKUP_ROOT / "20260318_101500",
        )

    def test_build_docker_pg_dump_command_uses_container_pg_dump(self):
        command = build_docker_pg_dump_command(
            container_name="event-news-pg",
            db_name="event_quant",
            db_user="postgres",
            db_password="postgres",
        )

        self.assertEqual(
            command,
            [
                "docker",
                "exec",
                "-e",
                "PGPASSWORD=postgres",
                "event-news-pg",
                "pg_dump",
                "-Fc",
                "-U",
                "postgres",
                "-d",
                "event_quant",
            ],
        )

    def test_write_backup_manifest_persists_backup_metadata(self):
        with TemporaryDirectory() as tmpdir:
            backup_dir = Path(tmpdir) / "20260318_101500"
            backup_dir.mkdir(parents=True, exist_ok=True)

            manifest_path = write_backup_manifest(
                backup_dir=backup_dir,
                backups=[
                    {"db_name": "event_news", "file_name": "event_news.dump", "bytes": 123},
                    {"db_name": "event_quant", "file_name": "event_quant.dump", "bytes": 456},
                ],
            )

            self.assertEqual(manifest_path, backup_dir / "manifest.json")
            payload = json.loads(manifest_path.read_text())
            self.assertEqual(payload["backup_dir"], str(backup_dir))
            self.assertEqual(len(payload["backups"]), 2)
            self.assertEqual(payload["backups"][0]["db_name"], "event_news")


if __name__ == "__main__":
    unittest.main()
