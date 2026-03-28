from __future__ import annotations

import argparse
import re
import sys
from html import unescape
from pathlib import Path

import psycopg

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.attribution_data import DEFAULT_EVENT_NEWS_DSN


OLD_HEADER = "| 时间 | 来源 | 标题 | 完整摘要/正文要点 | 链接 |"
NEW_HEADER = "| 时间 | 来源 | 标题 | 原文 | 链接 |"
SEPARATOR = "|---|---|---|---|---|"
def _strip_inline_code(text: str) -> str:
    text = text.strip()
    if text.startswith("`") and text.endswith("`"):
        return text[1:-1]
    return text


def _extract_markdown_link_url(text: str) -> str:
    text = text.strip()
    match = re.fullmatch(r"\[[^\]]*\]\(([^)]+)\)", text)
    if match:
        return match.group(1)
    return text


def _normalize_title(text: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", text).lower()


def _extract_match_segments(*texts: str) -> list[str]:
    segments: list[str] = []
    for text in texts:
        for piece in re.split(r"[，。；：、（）()【】\[\]\s\-—…,.!?:“”\"'#+]+", text):
            normalized = _normalize_title(piece)
            if len(normalized) >= 4:
                segments.append(normalized)
    return segments


def _split_markdown_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        raise ValueError(f"invalid markdown row: {line}")
    return [cell.strip() for cell in stripped.strip("|").split("|")]


def _build_markdown_row(values: list[str]) -> str:
    return "| " + " | ".join("" if value is None else str(value) for value in values) + " |"


def _render_news_markdown_section(rows: list[dict[str, str]]) -> list[str]:
    lines = [
        _build_markdown_row(["序号", "时间", "来源", "标题", "链接"]),
        SEPARATOR,
    ]
    for index, row in enumerate(rows, start=1):
        lines.append(
            _build_markdown_row(
                [
                    str(index),
                    row["published_at"],
                    f"`{row['source_id']}`",
                    row["title"],
                    f"[link]({row['url']})",
                ]
            )
        )
    lines.extend(["", "### 证据原文", ""])
    for index, row in enumerate(rows, start=1):
        lines.extend(
            [
                f"#### 证据 {index}",
                f"- 时间：`{row['published_at']}`",
                f"- 来源：`{row['source_id']}`",
                f"- 标题：{row['title']}",
                f"- 链接：[link]({row['url']})",
                "- 原文：",
                "```text",
                row["raw_text"],
                "```",
                "",
            ]
        )
    return lines[:-1] if lines and lines[-1] == "" else lines


def _parse_html_rows(block_text: str) -> list[dict[str, str]]:
    row_pattern = re.compile(
        r"(?s)<tr>\s*"
        r"<td>(?P<published_at>.*?)</td>\s*"
        r"<td><code>(?P<source_id>.*?)</code></td>\s*"
        r"<td>(?P<title>.*?)</td>\s*"
        r"<td><(?:div style=\"white-space: pre-wrap;\"|pre style=\"white-space: pre-wrap; margin: 0;\")>(?P<raw_text>.*?)</(?:div|pre)></td>\s*"
        r"<td><a href=\"(?P<url>[^\"]+)\">link</a></td>\s*"
        r"</tr>"
    )
    rows: list[dict[str, str]] = []
    for match in row_pattern.finditer(block_text):
        rows.append(
            {
                "published_at": unescape(match.group("published_at")).strip(),
                "source_id": unescape(match.group("source_id")).strip(),
                "title": unescape(match.group("title")).strip(),
                "raw_text": unescape(match.group("raw_text")).strip(),
                "url": unescape(match.group("url")).strip(),
            }
        )
    return rows


def _is_table_start_row(line: str) -> bool:
    return bool(re.match(r"^\|\s*\d{4}-\d{2}-\d{2}", line.strip()))


def _parse_multiline_row(row_lines: list[str]) -> dict[str, str]:
    if not row_lines:
        raise ValueError("empty markdown row")
    first_line = row_lines[0].strip()
    match = re.match(
        r"^\|\s*(?P<published_at>[^|]+?)\s*\|\s*(?P<source_id>[^|]+?)\s*\|\s*(?P<title>[^|]+?)\s*\|\s*(?P<tail>.*)$",
        first_line,
    )
    if match is None:
        raise ValueError(f"invalid markdown row: {row_lines[0]}")
    tail_lines = [match.group("tail"), *row_lines[1:]]
    tail_text = "\n".join(tail_lines).rstrip()
    tail_match = re.match(
        r"(?s)^(?P<old_text>.*?)(?:\s*\|\s*(?P<link>(?:\[[^\]]*\]\([^)]+\)|https?://\S+))\s*\|)\s*$",
        tail_text,
    )
    if tail_match is None:
        raise ValueError(f"invalid markdown row: {row_lines[0]}")
    published_at = match.group("published_at").strip()
    source_id = _strip_inline_code(match.group("source_id"))
    title = match.group("title").strip()
    old_text = tail_match.group("old_text").rstrip()
    url = _extract_markdown_link_url(tail_match.group("link"))
    return {
        "published_at": published_at,
        "source_id": source_id,
        "title": title,
        "old_text": old_text,
        "url": url,
    }


def find_news_table_blocks(markdown: str) -> list[tuple[int, int, int, list[dict[str, str]]]]:
    lines = markdown.splitlines()
    blocks: list[tuple[int, int, int, list[dict[str, str]]]] = []
    for header_index, line in enumerate(lines):
        if line.strip() not in {OLD_HEADER, NEW_HEADER}:
            continue
        if header_index + 1 >= len(lines) or lines[header_index + 1].strip() != SEPARATOR:
            continue
        rows: list[dict[str, str]] = []
        row_start = header_index + 2
        row_end = row_start
        current_row_lines: list[str] | None = None
        while row_end < len(lines):
            current_line = lines[row_end]
            if re.match(r"^#{2,6}\s+", current_line):
                break
            if _is_table_start_row(current_line):
                if current_row_lines is not None:
                    rows.append(_parse_multiline_row(current_row_lines))
                current_row_lines = [current_line]
                row_end += 1
                continue
            if current_row_lines is not None:
                current_row_lines.append(current_line)
                row_end += 1
                continue
            if current_line.strip() == "":
                row_end += 1
                continue
            break
        if current_row_lines is not None:
            rows.append(_parse_multiline_row(current_row_lines))
        blocks.append((header_index, row_start, row_end, rows))
    if not blocks:
        raise ValueError("news table header not found or already migrated")
    return blocks


def find_news_html_blocks(markdown: str) -> list[tuple[int, int, list[dict[str, str]]]]:
    lines = markdown.splitlines()
    blocks: list[tuple[int, int, list[dict[str, str]]]] = []
    for index, line in enumerate(lines):
        if not re.match(r"^#{2,6}\s+本地 news 库证据", line):
            continue
        table_start = None
        table_end = None
        cursor = index + 1
        while cursor < len(lines):
            current = lines[cursor]
            if re.match(r"^#{2,6}\s+", current):
                break
            if current.strip() == "<table>" and table_start is None:
                table_start = cursor
            if current.strip() == "</table>" and table_start is not None:
                table_end = cursor + 1
                break
            cursor += 1
        if table_start is None or table_end is None:
            continue
        block_text = "\n".join(lines[table_start:table_end])
        rows = _parse_html_rows(block_text)
        if rows:
            blocks.append((table_start, table_end, rows))
    return blocks


def parse_news_row_lookup_key(line: str) -> tuple[str, str, str, str]:
    published_at, source_id, title, _summary, url = _split_markdown_row(line)
    return (
        published_at,
        _strip_inline_code(source_id),
        title,
        _extract_markdown_link_url(url),
    )


def parse_news_row_record(line: str) -> dict[str, str]:
    published_at, source_id, title, old_text, url = _split_markdown_row(line)
    return {
        "published_at": published_at,
        "source_id": _strip_inline_code(source_id),
        "title": title,
        "old_text": old_text,
        "url": _extract_markdown_link_url(url),
    }


def migrate_report_markdown(markdown: str, raw_text_map: dict[tuple[str, str, str, str], str]) -> str:
    lines = markdown.splitlines()
    new_lines = list(lines)
    html_blocks = find_news_html_blocks(markdown)
    if html_blocks:
        for block_start, block_end, rows in reversed(html_blocks):
            replacement = _render_news_markdown_section(rows)
            new_lines[block_start:block_end] = replacement
        return "\n".join(new_lines) + ("\n" if markdown.endswith("\n") else "")

    blocks = find_news_table_blocks(markdown)
    for header_index, row_start, row_end, rows in reversed(blocks):
        rendered_rows = []
        for row in rows:
            key = (row["published_at"], row["source_id"], row["title"], row["url"])
            raw_text = raw_text_map[key]
            rendered_rows.append(
                {
                    "published_at": row["published_at"],
                    "source_id": row["source_id"],
                    "title": row["title"],
                    "raw_text": raw_text,
                    "url": row["url"],
                }
            )
        replacement = _render_news_markdown_section(rendered_rows)
        new_lines[header_index:row_end] = replacement

    return "\n".join(new_lines) + ("\n" if markdown.endswith("\n") else "")


def migrate_report_file(path: Path, raw_text_map: dict[tuple[str, str, str, str], str]) -> None:
    markdown = path.read_text(encoding="utf-8")
    migrated = migrate_report_markdown(markdown, raw_text_map)
    path.write_text(migrated, encoding="utf-8")


def collect_report_lookup_rows(markdown: str) -> list[dict[str, str]]:
    rows_out: list[dict[str, str]] = []
    for _header_index, _row_start, _row_end, rows in find_news_table_blocks(markdown):
        rows_out.extend(rows)
    return rows_out


def fetch_raw_text_map(conn, lookup_rows: list[dict[str, str]]) -> dict[tuple[str, str, str, str], str]:
    raw_text_map: dict[tuple[str, str, str, str], str] = {}
    sql_candidates = """
select coalesce(to_char(published_at at time zone 'Asia/Shanghai', 'YYYY-MM-DD HH24:MI'), to_char(published_at::date, 'YYYY-MM-DD')) as published_at_text,
       source_id,
       title,
       url,
       coalesce(summary, '') as raw_text
from event_metadata
where (published_at at time zone 'Asia/Shanghai')::date = %(published_date)s
  and source_id = %(source_id)s
order by published_at asc
""".strip()
    with conn.cursor() as cur:
        for row_info in lookup_rows:
            published_at = row_info["published_at"]
            source_id = row_info["source_id"]
            title = row_info["title"]
            old_text = row_info["old_text"]
            url = row_info["url"]
            params = {
                "published_date": published_at[:10],
                "source_id": source_id,
            }
            cur.execute(sql_candidates, params)
            candidates = cur.fetchall()
            row = None
            normalized_title = _normalize_title(title)
            for candidate in candidates:
                if candidate[3] == url:
                    row = candidate
                    break
            if row is None:
                for candidate in candidates:
                    candidate_title_norm = _normalize_title(candidate[2])
                    if candidate_title_norm == normalized_title:
                        row = candidate
                        break
            if row is None:
                for candidate in candidates:
                    candidate_title_norm = _normalize_title(candidate[2])
                    if normalized_title and (
                        normalized_title in candidate_title_norm or candidate_title_norm in normalized_title
                    ):
                        row = candidate
                        break
            if row is None:
                segments = _extract_match_segments(title, old_text)
                best_row = None
                best_score = 0
                for candidate in candidates:
                    candidate_text_norm = _normalize_title(f"{candidate[2]} {candidate[4]}")
                    score = sum(1 for segment in segments if segment in candidate_text_norm)
                    if score > best_score:
                        best_score = score
                        best_row = candidate
                if best_score > 0:
                    row = best_row
            if row is None:
                raise KeyError((published_at, source_id, title, url))
            _db_published_at, _db_source_id, _db_title, _db_url, raw_text = row
            raw_text_map[(published_at, source_id, title, url)] = raw_text
    return raw_text_map


def migrate_one_report(conn, report_path: Path) -> None:
    markdown = report_path.read_text(encoding="utf-8")
    html_blocks = find_news_html_blocks(markdown)
    if html_blocks:
        migrated = migrate_report_markdown(markdown, {})
        report_path.write_text(migrated, encoding="utf-8")
        return
    try:
        lookup_rows = collect_report_lookup_rows(markdown)
    except ValueError:
        raise
    raw_text_map = fetch_raw_text_map(conn, lookup_rows)
    migrate_report_file(report_path, raw_text_map)


def iter_report_paths(root: Path) -> list[Path]:
    return sorted(root.glob("*.md"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Migrate local news evidence tables to raw text")
    parser.add_argument("--file", dest="file_path")
    parser.add_argument("--all", action="store_true", dest="migrate_all")
    parser.add_argument("--dsn", default=DEFAULT_EVENT_NEWS_DSN)
    parser.add_argument("--analysis-dir", default="docs/analysis")
    args = parser.parse_args(argv)

    if not args.file_path and not args.migrate_all:
        raise SystemExit("must provide --file or --all")

    report_paths = [Path(args.file_path)] if args.file_path else iter_report_paths(Path(args.analysis_dir))
    with psycopg.connect(args.dsn) as conn:
        for report_path in report_paths:
            try:
                migrate_one_report(conn, report_path)
            except ValueError:
                if args.migrate_all:
                    print(f"SKIP {report_path}")
                    continue
                raise
            except KeyError:
                if args.migrate_all:
                    print(f"MISS {report_path}")
                    continue
                raise
            print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
