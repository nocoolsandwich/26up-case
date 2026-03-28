import importlib.util
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path("/Users/zhengshenghua/Library/Mobile Documents/com~apple~CloudDocs/work/my/case_data")
MODULE_PATH = PROJECT_ROOT / "scripts" / "migrate_news_raw_text_reports.py"


def load_module():
    spec = importlib.util.spec_from_file_location("migrate_news_raw_text_reports", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class MigrateNewsRawTextReportsTest(unittest.TestCase):
    def test_migrate_report_replaces_markdown_news_table_with_metadata_table_and_raw_text_blocks(self):
        module = load_module()
        markdown = """# 测试报告

## 本地 news 库证据

| 时间 | 来源 | 标题 | 完整摘要/正文要点 | 链接 |
|---|---|---|---|---|
| 2025-11-05 19:37 | zsxq_zhuwang | 小鹏科技日 | 老摘要 | https://example.com/1 |

## 量价与概念验证
"""
        raw_text_map = {
            ("2025-11-05 19:37", "zsxq_zhuwang", "小鹏科技日", "https://example.com/1"): "这里是数据库原文全文。"
        }

        migrated = module.migrate_report_markdown(markdown, raw_text_map)

        self.assertIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", migrated)
        self.assertIn("### 证据原文", migrated)
        self.assertIn("#### 证据 1", migrated)
        self.assertIn("```text", migrated)
        self.assertIn("这里是数据库原文全文。", migrated)
        self.assertNotIn("| 时间 | 来源 | 标题 | 完整摘要/正文要点 | 链接 |", migrated)

    def test_migrate_single_report_file_updates_markdown_in_place(self):
        module = load_module()
        raw_text_map = {
            ("2025-11-05 19:37", "zsxq_zhuwang", "小鹏科技日", "https://example.com/1"): "数据库原文"
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "sample.md"
            report_path.write_text(
                """## 本地 news 库证据

| 时间 | 来源 | 标题 | 完整摘要/正文要点 | 链接 |
|---|---|---|---|---|
| 2025-11-05 19:37 | zsxq_zhuwang | 小鹏科技日 | 老摘要 | https://example.com/1 |

## 备注
""",
                encoding="utf-8",
            )

            module.migrate_report_file(report_path, raw_text_map)

            migrated = report_path.read_text(encoding="utf-8")
            self.assertIn("数据库原文", migrated)
            self.assertIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", migrated)
            self.assertIn("### 证据原文", migrated)
            self.assertIn("```text", migrated)

    def test_migrate_report_handles_multiline_raw_text_rows_from_broken_markdown_table(self):
        module = load_module()
        markdown = """## 本地 news 库证据

| 时间 | 来源 | 标题 | 原文 | 链接 |
|---|---|---|---|---|
| 2025-11-05 17:12 | `zsxq_damao` | 小鹏机器人要点 | 小鹏机器人要点：

iron是第七代，从四足到类人再到更像人。

- 仿人脊椎
- 仿生肌肉

团队规模：10个团队。 | [link](https://example.com/robot) |

## 量价与概念验证
"""
        raw_text_map = {
            ("2025-11-05 17:12", "zsxq_damao", "小鹏机器人要点", "https://example.com/robot"): "数据库原文全文\n第二行"
        }

        migrated = module.migrate_report_markdown(markdown, raw_text_map)

        self.assertIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", migrated)
        self.assertIn("数据库原文全文\n第二行", migrated)
        self.assertIn("### 证据原文", migrated)
        self.assertIn("```text", migrated)
        self.assertNotIn("| 时间 | 来源 | 标题 | 原文 | 链接 |", migrated)
        self.assertNotIn("团队规模：10个团队。 | [link](https://example.com/robot) |", migrated)

    def test_migrate_report_converts_existing_html_news_table_to_metadata_table_and_raw_text_blocks(self):
        module = load_module()
        markdown = """## 本地 news 库证据

<table>
  <tbody>
  <tr>
    <td>2025-11-05 17:12</td>
    <td><code>zsxq_damao</code></td>
    <td>小鹏机器人要点</td>
    <td><pre style="white-space: pre-wrap; margin: 0;">数据库原文全文
第二行</pre></td>
    <td><a href="https://example.com/robot">link</a></td>
  </tr>
  </tbody>
</table>
"""

        migrated = module.migrate_report_markdown(markdown, {})

        self.assertIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", migrated)
        self.assertIn("### 证据原文", migrated)
        self.assertIn("#### 证据 1", migrated)
        self.assertIn("数据库原文全文\n第二行", migrated)
        self.assertIn("```text", migrated)
        self.assertNotIn("<table>", migrated)


if __name__ == "__main__":
    unittest.main()
