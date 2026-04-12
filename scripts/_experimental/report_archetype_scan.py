from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ANALYSIS_DIR = (
    Path(__file__).resolve().parents[2] / "outputs" / "analysis"
)

FILENAME_RE = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})-(?P<code>\d{6}(?:SZ|SH|TI))-(?P<name>.+)-wave-attribution\.md$"
)

ARCHETYPE_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("机器人", ("机器人", "丝杠", "轴承", "optimus", "iron", "谐波减速器")),
    ("AI算力硬件", ("pcb", "光通信", "光纤", "cpo", "ocs", "硅光", "ai数据中心", "aoc", "光模块")),
    ("存储", ("存储", "ddr", "hbm", "ssd", "ufs", "emmc", "长鑫", "长江存储")),
    ("资源品", ("锂", "锂矿", "碳酸锂", "供给出清", "价格反转")),
    ("电力设备", ("特高压", "hvdc", "海缆", "电缆", "国网投资", "供电", "燃气轮机", "发电机组")),
    ("商业航天/卫星", ("商业航天", "卫星", "手机直连", "星载", "太空算力", "空间电源")),
    ("半导体设备/先进封装", ("半导体设备", "先进封装", "检测", "封测", "hbm 检测", "量测")),
]


@dataclass
class ReportSummary:
    path: Path
    date: str
    code: str
    stock_name: str
    report_time: str
    one_liner: str
    archetype: str
    is_current_format: bool


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.MULTILINE)
    return match.group(1).strip() if match else ""


def infer_archetype(one_liner: str) -> str:
    candidate_text = one_liner
    if "而是" in one_liner:
        candidate_text = one_liner.rsplit("而是", 1)[-1]
    lowered = candidate_text.lower()
    hits: list[str] = []
    for label, keywords in ARCHETYPE_RULES:
        if any(keyword.lower() in lowered for keyword in keywords):
            hits.append(label)
    if not hits:
        return "其他"
    return " + ".join(hits[:2])


def parse_report(path: Path) -> ReportSummary | None:
    match = FILENAME_RE.match(path.name)
    if not match:
        return None
    text = path.read_text(encoding="utf-8", errors="ignore")
    one_liner = _extract(text, r"一句话逻辑：`([^`]+)`")
    report_time = _extract(text, r"报告时间：`([^`]+)`")
    is_current_format = all(
        marker in text
        for marker in (
            "## 证据原文",
            "## 量价验证表",
            "## 概念联动验证表",
            "## 综合裁决",
        )
    ) and "## 本地news归因" not in text
    return ReportSummary(
        path=path,
        date=match.group("date"),
        code=f"{match.group('code')[:6]}.{match.group('code')[6:]}",
        stock_name=match.group("name"),
        report_time=report_time,
        one_liner=one_liner,
        archetype=infer_archetype(one_liner),
        is_current_format=is_current_format,
    )


def load_latest_reports(analysis_dir: Path) -> list[ReportSummary]:
    latest_by_code: dict[str, ReportSummary] = {}
    for path in sorted(analysis_dir.glob("*-wave-attribution.md")):
        summary = parse_report(path)
        if summary is None:
            continue
        previous = latest_by_code.get(summary.code)
        if previous is None or (summary.date, summary.report_time) > (previous.date, previous.report_time):
            latest_by_code[summary.code] = summary
    return sorted(
        latest_by_code.values(),
        key=lambda item: (item.archetype, item.date, item.code),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan attribution reports and summarize archetypes.")
    parser.add_argument(
        "--analysis-dir",
        type=Path,
        default=DEFAULT_ANALYSIS_DIR,
        help="Directory containing wave attribution markdown reports.",
    )
    parser.add_argument(
        "--only-stale",
        action="store_true",
        help="Only print reports that are not on the current format.",
    )
    args = parser.parse_args()

    reports = load_latest_reports(args.analysis_dir)
    if args.only_stale:
        reports = [report for report in reports if not report.is_current_format]

    for report in reports:
        freshness = "current" if report.is_current_format else "stale"
        print(
            f"{report.archetype}\t{freshness}\t{report.code}\t{report.stock_name}\t"
            f"{report.date}\t{report.one_liner}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
