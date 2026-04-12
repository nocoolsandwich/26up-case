import json
import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = PROJECT_ROOT / "skills" / "stock-wave-attribution" / "scripts" / "project_skill.py"


class StockWaveAttributionProjectSkillTest(unittest.TestCase):
    def test_summary_reports_portable_runtime_files_and_dependency_chain(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "summary", "--json"],
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(result.stdout)

        self.assertEqual(payload["skill_name"], "stock-wave-attribution")
        self.assertEqual(payload["project_priority"], "portable-first")
        self.assertEqual(payload["runtime_owner"], "skill/orchestrator")
        self.assertIn("runtime/wave_plotting.py", payload["runtime_files"])
        self.assertIn("runtime/wave_segmentation.py", payload["runtime_files"])
        self.assertIn("runtime/attribution_data.py", payload["runtime_files"])
        self.assertIn("runtime/config.py", payload["runtime_files"])
        self.assertIn("stock-wave-attribution.yaml", payload["runtime_files"])
        self.assertIn("chatgpt-plus-browser", payload["skill_dependencies"])
        self.assertIn("outputs/analysis", payload["output_targets"])
        self.assertIn("wave review without web search", payload["planned_capabilities"])


if __name__ == "__main__":
    unittest.main()
