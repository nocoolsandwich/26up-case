import json
import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = PROJECT_ROOT / "skills" / "chatgpt-plus-browser" / "scripts" / "project_skill.py"


class ChatGPTPlusBrowserProjectSkillTest(unittest.TestCase):
    def test_summary_reports_project_first_migration_boundary(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "summary", "--json"],
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(result.stdout)

        self.assertEqual(payload["skill_name"], "chatgpt-plus-browser")
        self.assertEqual(payload["project_priority"], "project-first")
        self.assertEqual(payload["runtime_owner"], "skill/scripts")
        self.assertIn("docs/plans/2026-03-16-chatgpt-plus-browser-v2-design.md", payload["design_docs"])
        self.assertIn("skills/chatgpt-plus-browser/scripts", payload["skill_paths"])
        self.assertEqual(payload["project_module_paths"], [])
        self.assertIn("expected_patterns", payload["planned_capabilities"])


if __name__ == "__main__":
    unittest.main()
