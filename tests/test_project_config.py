import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


class ProjectConfigTest(unittest.TestCase):
    def test_load_project_config_reads_yaml_values(self):
        from scripts.project_config import load_project_config

        with TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            config_dir = project_root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "project.yaml").write_text(
                """
postgres:
  event_news_dsn: postgresql://localhost/event_news
  event_quant_dsn: postgresql://localhost/event_quant
tushare:
  token: demo-token
  http_url: http://example.com
paths:
  stock_file: stock.xlsx
  marco_file: marco.xlsx
  data_dir: data
  backup_root: data/db_backups
backup:
  mode: local
  docker:
    container_name: event-news-pg
case:
  sheet_name: 案例库
""".strip()
            )

            config = load_project_config(project_root)

            self.assertEqual(config["postgres"]["event_news_dsn"], "postgresql://localhost/event_news")
            self.assertEqual(config["tushare"]["token"], "demo-token")
            self.assertEqual(config["paths"]["stock_file"], "stock.xlsx")
            self.assertEqual(config["backup"]["mode"], "local")

    def test_load_project_config_requires_key_fields(self):
        from scripts.project_config import load_project_config

        with TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            config_dir = project_root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "project.yaml").write_text("postgres: {}\n")

            with self.assertRaises(ValueError):
                load_project_config(project_root)


if __name__ == "__main__":
    unittest.main()
