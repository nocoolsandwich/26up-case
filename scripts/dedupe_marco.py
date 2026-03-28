from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEDUP_KEYS = ["事件名称", "A股定价日T0", "类型"]
NUMERIC_COLS = ["T0上证涨跌幅", "T0深成涨跌幅", "T0创业板涨跌幅", "T0成交额（亿元）"]
TEXT_COLS = ["结构事实一句话（尽量客观）", "来源详情（写清政策+市场数据）", "工具/触点", "对指数方向（记录值）"]


def score_row(row: pd.Series) -> int:
    numeric_fill = sum(pd.notna(row.get(col)) for col in NUMERIC_COLS)
    text_len = sum(len(str(row.get(col))) for col in TEXT_COLS if pd.notna(row.get(col)))
    date_match = int(
        pd.notna(row.get("政策/事件日期"))
        and pd.notna(row.get("A股定价日T0"))
        and pd.to_datetime(row.get("政策/事件日期")) == pd.to_datetime(row.get("A股定价日T0"))
    )
    event_id = str(row.get("事件ID") or "")
    id_priority = 0
    if event_id.startswith("P-"):
        id_priority = 30
    elif event_id.startswith("W-"):
        id_priority = 20
    elif event_id.startswith("E-"):
        id_priority = 0
    return numeric_fill * 100 + date_match * 500 + id_priority + text_len


def dedupe_marco_df(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    keep_indices: list[int] = []
    removed_ids: list[str] = []
    grouped = df.groupby(DEDUP_KEYS, dropna=False, sort=False)

    for _, group in grouped:
        if len(group) == 1:
            keep_indices.append(group.index[0])
            continue
        scored = group.copy()
        scored["_score"] = scored.apply(score_row, axis=1)
        scored = scored.sort_values(["_score", "事件ID"], ascending=[False, True])
        keep_indices.append(scored.index[0])
        removed_ids.extend([str(x) for x in scored.iloc[1:]["事件ID"].tolist()])

    out = df.loc[sorted(keep_indices)].reset_index(drop=True)
    return out, removed_ids


def dedupe_file(input_path: Path, output_path: Path | None = None, sheet_name: str = "Sheet1") -> tuple[int, list[str]]:
    if output_path is None:
        output_path = input_path
    df = pd.read_excel(input_path, sheet_name=sheet_name)
    deduped, removed_ids = dedupe_marco_df(df)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        deduped.to_excel(writer, sheet_name=sheet_name, index=False)
    return len(removed_ids), removed_ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Dedupe marco.xlsx by event name + T0 + type")
    parser.add_argument("--input", default="marco.xlsx", help="Input xlsx path")
    parser.add_argument("--output", default="", help="Output xlsx path; overwrite input by default")
    parser.add_argument("--sheet", default="Sheet1", help="Sheet name")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output.strip() else None
    removed_count, removed_ids = dedupe_file(input_path, output_path, args.sheet)
    print(f"removed_rows={removed_count}")
    if removed_ids:
        print("removed_event_ids=" + ",".join(removed_ids))


if __name__ == "__main__":
    main()
