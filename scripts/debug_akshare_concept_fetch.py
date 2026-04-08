from __future__ import annotations

import argparse
import json
import logging
import os
import time
from contextlib import nullcontext
from typing import Callable

import akshare as ak
import pandas as pd

try:
    from scripts.event_quant_sync import (
        fetch_case_stock_concept_bundle_from_akshare,
        requests_sessions_without_proxy,
        resolve_akshare_stock_params,
    )
except ModuleNotFoundError:
    from event_quant_sync import (  # type: ignore
        fetch_case_stock_concept_bundle_from_akshare,
        requests_sessions_without_proxy,
        resolve_akshare_stock_params,
    )


logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="诊断 Akshare 概念板块接口和本地概念补库链路")
    parser.add_argument("--ts-code", default="300476.SZ", help="股票代码，例如 300476.SZ")
    parser.add_argument("--start-date", default="20250101", help="开始日期，格式 YYYYMMDD")
    parser.add_argument("--end-date", default="20260407", help="结束日期，格式 YYYYMMDD")
    parser.add_argument("--concept-name", default="", help="可选，直接测试的概念名称，例如 算力PCB")
    parser.add_argument("--concept-code", default="", help="可选，直接测试的概念代码，例如 BKxxxx")
    parser.add_argument(
        "--mode",
        choices=["both", "no-proxy", "default"],
        default="both",
        help="网络模式：同时测试、只测无代理、只测默认环境",
    )
    parser.add_argument("--preview", type=int, default=5, help="每个 DataFrame 最多打印前几行")
    return parser


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def env_proxy_snapshot() -> dict[str, str]:
    keys = [
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "no_proxy",
        "NO_PROXY",
    ]
    return {key: os.environ.get(key, "") for key in keys}


def dump_df(name: str, df: pd.DataFrame | None, preview: int) -> None:
    if df is None:
        logger.info("%s: None", name)
        return
    logger.info("%s: rows=%s columns=%s", name, len(df), list(df.columns))
    if df.empty:
        return
    records = df.head(preview).to_dict("records")
    logger.info("%s sample=%s", name, json.dumps(records, ensure_ascii=False, default=str, indent=2))


def run_step(label: str, func: Callable[[], object]) -> object | None:
    start = time.perf_counter()
    logger.info("开始步骤: %s", label)
    try:
        result = func()
    except Exception:
        elapsed = time.perf_counter() - start
        logger.error("步骤失败: %s elapsed=%.3fs", label, elapsed, exc_info=True)
        return None
    elapsed = time.perf_counter() - start
    logger.info("步骤成功: %s elapsed=%.3fs", label, elapsed)
    return result


def detect_hist_symbol(
    concept_name_df: pd.DataFrame | None,
    concept_name: str,
    concept_code: str,
) -> str:
    if concept_name:
        return concept_name
    if concept_name_df is None or concept_name_df.empty or not concept_code:
        return ""
    hit = concept_name_df[concept_name_df["板块代码"].astype(str) == str(concept_code)]
    if hit.empty:
        return ""
    return str(hit.iloc[0]["板块名称"])


def inspect_raw_akshare(
    *,
    ts_code: str,
    start_date: str,
    end_date: str,
    concept_name: str,
    concept_code: str,
    preview: int,
) -> None:
    stock_symbol, _ = resolve_akshare_stock_params(ts_code)
    concept_name_df = run_step("ak.stock_board_concept_name_em", ak.stock_board_concept_name_em)
    if isinstance(concept_name_df, pd.DataFrame):
        dump_df("concept_name_em", concept_name_df, preview)

    cons_symbol = concept_code or concept_name
    if cons_symbol:
        cons_df = run_step(
            f"ak.stock_board_concept_cons_em(symbol={cons_symbol})",
            lambda: ak.stock_board_concept_cons_em(symbol=cons_symbol),
        )
        if isinstance(cons_df, pd.DataFrame):
            dump_df("concept_cons_em", cons_df, preview)
            if "代码" in cons_df.columns:
                codes = cons_df["代码"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
                logger.info("成分股是否命中 %s: %s", stock_symbol, stock_symbol in set(codes.tolist()))

    hist_symbol = detect_hist_symbol(
        concept_name_df if isinstance(concept_name_df, pd.DataFrame) else None,
        concept_name=concept_name,
        concept_code=concept_code,
    )
    if hist_symbol:
        hist_df = run_step(
            f"ak.stock_board_concept_hist_em(symbol={hist_symbol})",
            lambda: ak.stock_board_concept_hist_em(
                symbol=hist_symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="",
            ),
        )
        if isinstance(hist_df, pd.DataFrame):
            dump_df("concept_hist_em", hist_df, preview)


def inspect_bundle(
    *,
    ts_code: str,
    start_date: str,
    end_date: str,
    preview: int,
) -> None:
    bundle = run_step(
        "fetch_case_stock_concept_bundle_from_akshare",
        lambda: fetch_case_stock_concept_bundle_from_akshare(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        ),
    )
    if not isinstance(bundle, dict):
        return
    logger.info("bundle tables=%s", list(bundle.keys()))
    for table_name, frame in bundle.items():
        if isinstance(frame, pd.DataFrame):
            dump_df(table_name, frame, preview)
        else:
            logger.info("%s: type=%s", table_name, type(frame).__name__)


def run_mode(
    *,
    mode_name: str,
    context_factory: Callable[[], object],
    args: argparse.Namespace,
) -> None:
    logger.info("========== mode=%s ==========", mode_name)
    logger.info("proxy_env=%s", json.dumps(env_proxy_snapshot(), ensure_ascii=False, indent=2))
    with context_factory():
        inspect_raw_akshare(
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            concept_name=args.concept_name,
            concept_code=args.concept_code,
            preview=args.preview,
        )
        inspect_bundle(
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            preview=args.preview,
        )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging()

    if args.mode in {"both", "no-proxy"}:
        run_mode(
            mode_name="no-proxy",
            context_factory=requests_sessions_without_proxy,
            args=args,
        )

    if args.mode in {"both", "default"}:
        run_mode(
            mode_name="default",
            context_factory=nullcontext,
            args=args,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
