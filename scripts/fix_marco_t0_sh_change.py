from __future__ import annotations

import argparse
import time
from pathlib import Path

import akshare as ak
import pandas as pd


def build_sh_change_map(index_df: pd.DataFrame) -> dict[str, float]:
    df = index_df.copy()
    if "date" not in df.columns or "close" not in df.columns:
        raise ValueError("index_df must contain 'date' and 'close' columns")
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values("date")
    df["pct"] = df["close"].astype(float).pct_change()
    df = df.dropna(subset=["pct"])
    return dict(zip(df["date"], df["pct"]))


def build_amount_map(index_df: pd.DataFrame) -> dict[str, float]:
    df = index_df.copy()
    if "date" not in df.columns or "amount" not in df.columns:
        return {}
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    # Tencent index amount unit is typically 万元; convert to 亿元 => / 1e5.
    df["amount_yi"] = df["amount"].astype(float) / 1e5
    return dict(zip(df["date"], df["amount_yi"]))


def merge_index_data(primary_df: pd.DataFrame, fallback_df: pd.DataFrame) -> pd.DataFrame:
    """Merge two index series by date with primary priority and fallback fill."""
    p = primary_df.copy()
    f = fallback_df.copy()
    p["date"] = pd.to_datetime(p["date"]).dt.strftime("%Y-%m-%d")
    f["date"] = pd.to_datetime(f["date"]).dt.strftime("%Y-%m-%d")
    p = p.sort_values("date").drop_duplicates("date", keep="last")
    f = f.sort_values("date").drop_duplicates("date", keep="last")
    merged = p.set_index("date")
    for col in f.columns:
        if col == "date":
            continue
        if col not in merged.columns:
            merged[col] = pd.NA
    fallback = f.set_index("date")
    for col in merged.columns:
        if col in fallback.columns:
            merged[col] = merged[col].where(merged[col].notna(), fallback[col])
    # include dates that exist only in fallback
    extra_dates = fallback.index.difference(merged.index)
    if len(extra_dates) > 0:
        extra = fallback.loc[extra_dates]
        for col in merged.columns:
            if col not in extra.columns:
                extra[col] = pd.NA
        merged = pd.concat([merged, extra[merged.columns]], axis=0)
    merged = merged.reset_index().rename(columns={"index": "date"}).sort_values("date")
    return merged


def apply_t0_sh_change(df: pd.DataFrame, sh_change_map: dict[str, float], target_col: str) -> tuple[pd.DataFrame, int, list[str]]:
    out = df.copy()
    if "A股定价日T0" not in out.columns or target_col not in out.columns:
        raise ValueError(f"missing required columns: A股定价日T0 / {target_col}")

    changed = 0
    missing: list[str] = []

    for i, val in out["A股定价日T0"].items():
        if pd.isna(val):
            continue
        key = pd.to_datetime(val).strftime("%Y-%m-%d")
        if key in sh_change_map:
            new_v = float(sh_change_map[key])
            old_v = out.at[i, target_col]
            try:
                old_v_num = float(old_v)
            except Exception:
                old_v_num = float("nan")
            if pd.isna(old_v_num) or abs(old_v_num - new_v) > 1e-12:
                out.at[i, target_col] = new_v
                changed += 1
        else:
            missing.append(key)

    missing = sorted(set(missing))
    return out, changed, missing


def apply_t0_amount(df: pd.DataFrame, amount_map: dict[str, float], target_col: str = "T0成交额（亿元）") -> tuple[pd.DataFrame, int, list[str]]:
    out = df.copy()
    if "A股定价日T0" not in out.columns or target_col not in out.columns:
        raise ValueError(f"missing required columns: A股定价日T0 / {target_col}")

    changed = 0
    missing: list[str] = []
    for i, val in out["A股定价日T0"].items():
        if pd.isna(val):
            continue
        key = pd.to_datetime(val).strftime("%Y-%m-%d")
        if key in amount_map:
            new_v = float(amount_map[key])
            old_v = out.at[i, target_col]
            try:
                old_v_num = float(str(old_v).split("（")[0]) if not pd.isna(old_v) else float("nan")
            except Exception:
                old_v_num = float("nan")
            if pd.isna(old_v_num) or abs(old_v_num - new_v) > 1e-8:
                out.at[i, target_col] = new_v
                changed += 1
        else:
            missing.append(key)
    return out, changed, sorted(set(missing))


def fetch_index_df(symbol: str, max_retries: int = 3) -> pd.DataFrame:
    last_err: Exception | None = None
    for _ in range(max_retries):
        try:
            return ak.stock_zh_index_daily_tx(symbol=symbol)
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    for _ in range(max_retries):
        try:
            return ak.stock_zh_index_daily(symbol=symbol)
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    if last_err is not None:
        raise last_err
    raise RuntimeError("failed to fetch sh index data")


def fetch_index_df_with_fallback(symbol: str) -> pd.DataFrame:
    """TX first; fallback to generic daily for historical coverage gaps."""
    tx_df = fetch_index_df(symbol)
    try:
        fb = ak.stock_zh_index_daily(symbol=symbol)
        return merge_index_data(tx_df, fb)
    except Exception:
        return tx_df


def fix_file(input_path: Path, output_path: Path | None = None, sheet_name: str = "Sheet1") -> tuple[int, dict[str, list[str]]]:
    if output_path is None:
        output_path = input_path

    df = pd.read_excel(input_path, sheet_name=sheet_name)

    sh_idx = fetch_index_df_with_fallback("sh000001")
    sz_idx = fetch_index_df_with_fallback("sz399001")
    cyb_idx = fetch_index_df_with_fallback("sz399006")

    sh_map = build_sh_change_map(sh_idx)
    sz_map = build_sh_change_map(sz_idx)
    cyb_map = build_sh_change_map(cyb_idx)

    new_df, ch_sh, ms_sh = apply_t0_sh_change(df, sh_map, "T0上证涨跌幅")
    new_df, ch_sz, ms_sz = apply_t0_sh_change(new_df, sz_map, "T0深成涨跌幅")
    new_df, ch_cyb, ms_cyb = apply_t0_sh_change(new_df, cyb_map, "T0创业板涨跌幅")

    # Use SH+SZ amount (亿元) as a stable market turnover proxy.
    sh_amt = build_amount_map(sh_idx)
    sz_amt = build_amount_map(sz_idx)
    t0_amt = {d: sh_amt[d] + sz_amt[d] for d in sh_amt.keys() & sz_amt.keys()}
    new_df, ch_amt, ms_amt = apply_t0_amount(new_df, t0_amt, "T0成交额（亿元）")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        new_df.to_excel(writer, sheet_name=sheet_name, index=False)

    changed = ch_sh + ch_sz + ch_cyb + ch_amt
    missing = {
        "T0上证涨跌幅": sorted(set(ms_sh)),
        "T0深成涨跌幅": sorted(set(ms_sz)),
        "T0创业板涨跌幅": sorted(set(ms_cyb)),
        "T0成交额（亿元）": sorted(set(ms_amt)),
    }
    return changed, missing


def main() -> None:
    parser = argparse.ArgumentParser(description="修订 marco.xlsx 的 T0 指标（上证/深成/创业板涨跌幅 + 成交额）")
    parser.add_argument("--input", default="marco.xlsx", help="输入 xlsx 路径")
    parser.add_argument("--output", default="", help="输出 xlsx 路径，默认覆盖输入")
    parser.add_argument("--sheet", default="Sheet1", help="工作表名")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output.strip() else None

    changed, missing = fix_file(input_path=input_path, output_path=output_path, sheet_name=args.sheet)
    print(f"updated_rows={changed}")
    for k, v in missing.items():
        print(f"missing_{k}={len(v)}")
        if v:
            print(f"missing_dates_{k}=" + ",".join(v[:20]))


if __name__ == "__main__":
    main()
