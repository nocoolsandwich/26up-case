from __future__ import annotations

from pathlib import Path

from service.models import AttributionTask
from service.result_locator import locate_task_result


def test_locate_task_result_returns_expected_paths(tmp_path: Path) -> None:
    report_path = tmp_path / "docs" / "analysis" / "demo.md"
    plot_path = tmp_path / "data" / "plots" / "demo.png"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# demo", encoding="utf-8")
    plot_path.write_text("png", encoding="utf-8")

    task = AttributionTask(
        task_id="attr-006",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="数据中心",
        report_path=str(report_path),
        plot_path=str(plot_path),
        chatgpt_task_id="chatgpt-001",
    )

    result = locate_task_result(task)

    assert result["report_exists"] is True
    assert result["plot_exists"] is True
    assert result["report_path"] == str(report_path)
    assert result["plot_path"] == str(plot_path)
    assert result["chatgpt_task_id"] == "chatgpt-001"


def test_locate_task_result_can_fallback_to_latest_report_pattern(tmp_path: Path) -> None:
    report_dir = tmp_path / "docs" / "analysis"
    plot_dir = tmp_path / "data" / "plots"
    report_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "2026-03-26-688195SH-腾景科技-wave-attribution.md"
    plot_path = plot_dir / "688195_SH_wave_candles.png"
    report_path.write_text("# demo", encoding="utf-8")
    plot_path.write_text("png", encoding="utf-8")

    task = AttributionTask(
        task_id="attr-007",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="数据中心",
    )

    result = locate_task_result(task, workspace_root=tmp_path)

    assert result["report_exists"] is True
    assert result["plot_exists"] is True
    assert result["report_path"].endswith("2026-03-26-688195SH-腾景科技-wave-attribution.md")
    assert result["plot_path"].endswith("688195_SH_wave_candles.png")


def test_locate_task_result_can_extract_chatgpt_task_id_from_report(tmp_path: Path) -> None:
    report_dir = tmp_path / "docs" / "analysis"
    plot_dir = tmp_path / "data" / "plots"
    report_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "2026-03-25-688375SH-国博电子-wave-attribution.md"
    plot_path = plot_dir / "688375_SH_wave_candles.png"
    report_path.write_text(
        "\n".join(
            [
                "### ChatGPT 联网归因",
                "- task id：",
                "  `d7b0a15f-688a-41af-b738-ec8d9ab5290a`",
                "- 结果文件：",
                "  `skills/chatgpt-plus-browser/.state/d7b0a15f-688a-41af-b738-ec8d9ab5290a.json`",
            ]
        ),
        encoding="utf-8",
    )
    plot_path.write_text("png", encoding="utf-8")

    task = AttributionTask(
        task_id="attr-008",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-12-10",
        end_date="2026-01-14",
        sample_label="5G",
    )

    result = locate_task_result(task, workspace_root=tmp_path)

    assert result["chatgpt_task_id"] == "d7b0a15f-688a-41af-b738-ec8d9ab5290a"
