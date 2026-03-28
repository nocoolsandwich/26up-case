from __future__ import annotations

from typing import Any

import pandas as pd


def segment_ma_trend_waves(
    df: pd.DataFrame,
    price_col: str = "close_qfq",
    ma_window: int = 10,
    signal_window: int = 20,
    negative_streak: int = 5,
    positive_streak: int = 3,
    below_signal_streak: int = 3,
    min_bars: int = 8,
) -> list[dict[str, Any]]:
    if df.empty:
        return []

    ordered = df.sort_values("trade_date").reset_index(drop=True).copy()
    ordered[price_col] = ordered[price_col].astype(float)
    ordered["ma_fast"] = ordered[price_col].rolling(ma_window, min_periods=1).mean()
    ordered["ma_signal"] = ordered[price_col].rolling(signal_window, min_periods=1).mean()
    ordered["ma_fast_diff"] = ordered["ma_fast"].diff()
    ordered["fast_positive"] = ordered["ma_fast_diff"] > 0
    ordered["fast_negative"] = ordered["ma_fast_diff"] < 0
    ordered["below_signal"] = ordered[price_col] < ordered["ma_signal"]

    waves: list[dict[str, Any]] = []
    active_start: int | None = None
    peak_idx: int | None = None
    pos_count = 0
    neg_count = 0
    below_count = 0

    for idx in range(len(ordered)):
        ma_fast = ordered.loc[idx, "ma_fast"]
        ma_signal = ordered.loc[idx, "ma_signal"]
        price = ordered.loc[idx, price_col]
        if pd.isna(ma_fast):
            continue

        pos_count = pos_count + 1 if ordered.loc[idx, "fast_positive"] else 0
        neg_count = neg_count + 1 if ordered.loc[idx, "fast_negative"] else 0
        below_count = below_count + 1 if ordered.loc[idx, "below_signal"] else 0

        signal_ready = not pd.isna(ma_signal)

        if active_start is None:
            if pos_count >= positive_streak and price >= ma_fast and (not signal_ready or price >= ma_signal):
                active_start = idx - positive_streak + 1
                peak_idx = idx
            continue

        if peak_idx is None or price >= ordered.loc[peak_idx, price_col]:
            peak_idx = idx

        bar_count = idx - active_start + 1
        if bar_count >= min_bars and (neg_count >= negative_streak or below_count >= below_signal_streak):
            end_idx = idx - max(neg_count, below_count)
            wave_bars = peak_idx - active_start + 1 if peak_idx is not None else 0
            if peak_idx is not None and peak_idx >= active_start and wave_bars >= min_bars:
                waves.append(
                    _build_wave_record(
                        ordered=ordered,
                        prices=ordered[price_col],
                        trough_idx=active_start,
                        peak_idx=peak_idx,
                        price_col=price_col,
                    )
                )
            active_start = None
            peak_idx = None
            pos_count = 0
            neg_count = 0
            below_count = 0

    if active_start is not None and peak_idx is not None and peak_idx - active_start + 1 >= min_bars:
        waves.append(
            _build_wave_record(
                ordered=ordered,
                prices=ordered[price_col],
                trough_idx=active_start,
                peak_idx=peak_idx,
                price_col=price_col,
            )
        )

    return _dedupe_overlapping_waves(waves)


def segment_price_waves(
    df: pd.DataFrame,
    price_col: str = "close_qfq",
    min_wave_gain: float = 0.35,
    min_pullback: float = 0.15,
    min_bars: int = 4,
) -> list[dict[str, Any]]:
    if df.empty:
        return []

    ordered = df.sort_values("trade_date").reset_index(drop=True)
    prices = ordered[price_col].astype(float)

    trough_idx = 0
    peak_idx = 0
    waves: list[dict[str, Any]] = []

    for idx in range(1, len(ordered)):
        if prices.iloc[idx] >= prices.iloc[peak_idx]:
            peak_idx = idx

        peak_price = prices.iloc[peak_idx]
        trough_price = prices.iloc[trough_idx]
        gain = (peak_price - trough_price) / trough_price if trough_price else 0.0
        pullback = (peak_price - prices.iloc[idx]) / peak_price if peak_price else 0.0
        bar_count = peak_idx - trough_idx + 1

        if gain >= min_wave_gain and pullback >= min_pullback and bar_count >= min_bars:
            waves.append(
                _build_wave_record(
                    ordered=ordered,
                    prices=prices,
                    trough_idx=trough_idx,
                    peak_idx=peak_idx,
                    price_col=price_col,
                )
            )
            trough_idx = idx
            peak_idx = idx
            continue

        if peak_idx == trough_idx and prices.iloc[idx] < prices.iloc[trough_idx]:
            trough_idx = idx
            peak_idx = idx

    peak_price = prices.iloc[peak_idx]
    trough_price = prices.iloc[trough_idx]
    gain = (peak_price - trough_price) / trough_price if trough_price else 0.0
    bar_count = peak_idx - trough_idx + 1
    if gain >= min_wave_gain and bar_count >= min_bars:
        waves.append(
            _build_wave_record(
                ordered=ordered,
                prices=prices,
                trough_idx=trough_idx,
                peak_idx=peak_idx,
                price_col=price_col,
            )
        )

    return waves


def _build_wave_record(
    ordered: pd.DataFrame,
    prices: pd.Series,
    trough_idx: int,
    peak_idx: int,
    price_col: str,
) -> dict[str, Any]:
    start_price = float(prices.iloc[trough_idx])
    peak_price = float(prices.iloc[peak_idx])
    return {
        "start_idx": trough_idx,
        "peak_idx": peak_idx,
        "start_date": str(ordered.iloc[trough_idx]["trade_date"]),
        "peak_date": str(ordered.iloc[peak_idx]["trade_date"]),
        "start_price": start_price,
        "peak_price": peak_price,
        "wave_gain_pct": (peak_price / start_price - 1.0) * 100.0,
        "bars": peak_idx - trough_idx + 1,
        "price_col": price_col,
    }


def _dedupe_overlapping_waves(waves: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not waves:
        return []

    deduped = [waves[0]]
    for wave in waves[1:]:
        prev = deduped[-1]
        if wave["start_idx"] <= prev["peak_idx"]:
            if wave["wave_gain_pct"] > prev["wave_gain_pct"]:
                deduped[-1] = wave
            continue
        deduped.append(wave)
    return deduped
