"""Ratio Spread Candle Calculator for CryptoSpread Web Chart.

High-performance, pure Python calculator for generating Ratio Spread candles
from dual-exchange OHLCV time series.

Arbitrage Spread Formula:
    Ratio = Sell Price / Buy Price
    Deviation = (Ratio - 1.0) * 100% (Positive spread = Arbitrage Profit)
    Convergence Target = 1.0000 (0% Spread)
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def calculate_ratio_candles(
    buy_ohlcv: List[List[Any]],
    sell_ohlcv: List[List[Any]],
) -> List[Dict[str, float | int]]:
    """Calculate Ratio Spread candles (Sell Exchange / Buy Exchange) from OHLCV data.

    Calculates:
        ratio_open  = sell_open / buy_open
        ratio_high  = max possible ratio in candle = max(sell_high / buy_low, ratio_open, ratio_close)
        ratio_low   = min possible ratio in candle = min(sell_low / buy_high, ratio_open, ratio_close)
        ratio_close = sell_close / buy_close

    Args:
        buy_ohlcv: List of OHLCV candles from buy exchange [[timestamp, o, h, l, c, v], ...].
        sell_ohlcv: List of OHLCV candles from sell exchange [[timestamp, o, h, l, c, v], ...].

    Returns:
        List of candle dicts formatted for TradingView Lightweight Charts:
        [{"time": int (UNIX seconds), "open": float, "high": float, "low": float, "close": float}, ...]
    """
    if not buy_ohlcv or not sell_ohlcv:
        logger.warning("Empty OHLCV data provided for ratio calculation.")
        return []

    # 1. Normalize timestamps to UNIX integer seconds
    def to_sec(ts: float | int) -> int:
        return int(ts // 1000 if ts > 1e11 else ts)

    buy_map: Dict[int, List[float]] = {}
    for candle in buy_ohlcv:
        if len(candle) >= 5 and candle[0] is not None:
            try:
                sec = to_sec(candle[0])
                if all(candle[i] is not None for i in range(1, 5)):
                    buy_map[sec] = [float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4])]
            except (TypeError, ValueError):
                continue

    sell_map: Dict[int, List[float]] = {}
    for candle in sell_ohlcv:
        if len(candle) >= 5 and candle[0] is not None:
            try:
                sec = to_sec(candle[0])
                if all(candle[i] is not None for i in range(1, 5)):
                    sell_map[sec] = [float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4])]
            except (TypeError, ValueError):
                continue

    all_times = sorted(set(buy_map.keys()) | set(sell_map.keys()))
    if not all_times:
        return []

    # 2. Forward fill aligned time series
    last_buy: List[float] | None = None
    last_sell: List[float] | None = None
    result: List[Dict[str, float | int]] = []

    for t in all_times:
        if t in buy_map:
            last_buy = buy_map[t]
        if t in sell_map:
            last_sell = sell_map[t]

        # Only calculate when both exchanges have had at least one valid candle
        if last_buy is not None and last_sell is not None:
            b_o, b_h, b_l, b_c = last_buy
            s_o, s_h, s_l, s_c = last_sell

            # Guard against zero division
            if b_o <= 0 or b_l <= 0 or b_h <= 0 or b_c <= 0:
                continue
            if s_o <= 0 or s_l <= 0 or s_h <= 0 or s_c <= 0:
                continue

            # Arbitrage Percentage Spread: ((Sell Side / Buy Side) - 1.0) * 100.0%
            spread_o = ((s_o / b_o) - 1.0) * 100.0
            spread_h = ((s_h / b_l) - 1.0) * 100.0
            spread_l = ((s_l / b_h) - 1.0) * 100.0
            spread_c = ((s_c / b_c) - 1.0) * 100.0

            # Enforce OHLC consistency in percentage space
            candle_high = max(spread_h, spread_o, spread_c)
            candle_low = min(spread_l, spread_o, spread_c)

            result.append({
                "time": t,
                "open": round(spread_o, 4),
                "high": round(candle_high, 4),
                "low": round(candle_low, 4),
                "close": round(spread_c, 4),
            })

    return result
