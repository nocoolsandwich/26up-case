from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


SOURCE_COL = "来源详情（写清政策+市场数据）"
PRIORITY_IDS = {
    "P-2024-0711",
    "P-2024-04-GJTL",
    "P-2023-0828",
    "P-2023-0217",
    "P-2022-0224",
    "P-2019-0722",
    "P-2016-0108",
    "P-2015-0818",
    "P-2015-0708-A",
    "P-2015-0708-B",
    "P-2014-HKSC",
    "P-2008-0919",
    "P-2008-0424",
    "P-2007-530",
    "E-2024-0924",
    "E-2024-1105",
    "E-2024-1122",
    "E-2026-0202",
}


def find_issues(text: object) -> list[str]:
    s = "" if pd.isna(text) else str(text).strip()
    if not s:
        return ["来源详情为空"]

    issues: list[str] = []
    direct_flags = [
        ("口径见来源", "含“口径见来源”"),
        ("见来源", "含“见来源”"),
        ("同上", "含“同上”"),
        ("相关安排", "含“相关安排”"),
        ("背景与政策要点转述", "政策表述偏转述"),
        ("并无单一", "缺单一事件锚点"),
    ]
    for pat, label in direct_flags:
        if pat in s:
            issues.append(label)

    # Only flag judgmental wording when the sentence lacks a concrete event/policy anchor.
    has_anchor = ("事件：" in s) or ("政策：" in s)
    soft_flags = [
        ("样本", "样本化表述"),
        ("更偏", "判断词“更偏”"),
        ("更接近", "判断词“更接近”"),
        ("更适合作为", "判断词“更适合作为”"),
        ("市场解读集中在", "表述偏总结"),
    ]
    if not has_anchor:
        for pat, label in soft_flags:
            if pat in s:
                issues.append(label)

    if len(s) < 40:
        issues.append("来源详情过短")
    if "市场：" not in s:
        issues.append("缺市场段落")
    if not has_anchor:
        issues.append("缺事件/政策锚点")
    if not re.search(r"\d{4}-\d{2}-\d{2}|\d月\d日", s) and ("当日" in s):
        issues.append("时间表达偏泛")
    if not re.search(r"沪指|上证|深成指|创业板|成交额|上涨|下跌|个股", s):
        issues.append("缺明确市场事实")

    return list(dict.fromkeys(issues))


def build_review_df(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        issues = find_issues(row.get(SOURCE_COL))
        if not issues:
            continue
        out = row.to_dict()
        out["修订优先级"] = "高" if str(row.get("事件ID")) in PRIORITY_IDS else "中"
        out["问题标签"] = "；".join(issues)
        out["建议修订口径"] = "事件本体（谁/何时/做了什么） + 市场定价（指数/成交额/广度/领涨领跌）"
        rows.append(out)

    if not rows:
        return pd.DataFrame(columns=["修订优先级", "问题标签", "建议修订口径"])

    review_df = pd.DataFrame(rows)
    front = ["修订优先级", "问题标签", "建议修订口径"]
    other = [c for c in review_df.columns if c not in front]
    review_df = review_df[front + other]
    review_df["__prio"] = review_df["修订优先级"].map({"高": 0, "中": 1}).fillna(9)
    review_df = review_df.sort_values(["__prio", "A股定价日T0", "事件ID"]).drop(columns="__prio")
    return review_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Build marco source detail review sheet")
    parser.add_argument("--input", default="marco.xlsx", help="Input xlsx path")
    parser.add_argument("--output-xlsx", default="marco_source_detail_review.xlsx", help="Output xlsx path")
    parser.add_argument("--output-csv", default="marco_source_detail_review.csv", help="Output csv path")
    parser.add_argument("--sheet", default="Sheet1", help="Input sheet name")
    args = parser.parse_args()

    input_path = Path(args.input)
    out_xlsx = Path(args.output_xlsx)
    out_csv = Path(args.output_csv)

    df = pd.read_excel(input_path, sheet_name=args.sheet)
    review_df = build_review_df(df)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        review_df.to_excel(writer, sheet_name="来源详情待修订", index=False)
    review_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"rows={len(review_df)}")
    print(f"xlsx={out_xlsx}")
    print(f"csv={out_csv}")


if __name__ == "__main__":
    main()
