from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_manifest(skill_root: Path) -> dict:
    manifest_path = skill_root / "skill_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["project_root"] = str(skill_root.parents[1])
    return payload


def render_text_summary(payload: dict) -> str:
    lines = [
        f"skill: {payload['skill_name']}",
        f"priority: {payload['project_priority']}",
        f"runtime_owner: {payload['runtime_owner']}",
        "project_modules:",
    ]
    lines.extend(payload["project_module_paths"] or ["<none>"])
    lines.append("planned_capabilities:")
    lines.extend(payload["planned_capabilities"])
    return "\n".join(lines)


def main(skill_root: Path) -> int:
    parser = argparse.ArgumentParser(description="Project-local skill summary")
    subparsers = parser.add_subparsers(dest="command", required=True)
    summary_parser = subparsers.add_parser("summary", help="Print skill summary")
    summary_parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    payload = load_manifest(skill_root)
    if args.command == "summary":
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(render_text_summary(payload))
        return 0
    return 1
