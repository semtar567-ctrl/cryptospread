"""Async CCXT Fetcher module for CryptoSpread Web Chart.

Fetches OHLCV candles from supported cryptocurrency exchanges concurrently
using CCXT's async support with direct REST fallbacks and linear swap prioritization.
"""

import asyncio
import logging
from typing import Any

import aiohttp
import ccxt.async_support as ccxt

logger = logging.getLogger(__name__)

# Exchange aliases mapping from CryptoSpread internal names to CCXT class names
EXCHANGE_MAP = {
    "gateio": "gate",
    "kucoinfutures": "kucoinfutures",
    "coinexfutures": "coinex",
}


async def fetch_asterdex_direct_candles(symbol: str, timeframe: str = "1m", limit: int = 300) -> list[list[Any]]:
    """Direct high-speed REST fetcher for Aster DEX swap candles."""
    clean = symbol.replace("/", "").replace(":USDT", "").replace(":USDC", "").upper()
    if not clean.endswith("USDT") and not clean.endswith("USDC"):
        clean = f"{clean}USDT"
    
    tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
    interval = tf_map.get(timeframe.lower(), "1m")
    
    url = f"https://fapi.asterdex.com/fapi/v1/klines?symbol={clean}&interval={interval}&limit={limit}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                candles = []
                for c in data:
                    candles.append([int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])])
                return candles
            else:
                logger.warning(f"Aster DEX direct API response code: {resp.status}")
    return []


async def fetch_okx_direct_candles(symbol: str, timeframe: str = "1m", limit: int = 300) -> list[list[Any]]:
    """Direct high-speed REST fetcher for OKX swap candles (bypasses CCXT market-loading issues)."""
    clean = symbol.replace("/", "").replace(":USDT", "").replace(":USDC", "").upper()
    if clean.endswith("USDT"):
        base = clean[:-4]
    elif clean.endswith("USDC"):
        base = clean[:-4]
    else:
        base = clean
    
    inst_id = f"{base}-USDT-SWAP"
    tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
    bar = tf_map.get(timeframe.lower(), "1m")
    
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            data = await resp.json()
            if data.get("code") == "0" and data.get("data"):
                candles = []
                for c in reversed(data["data"]):
                    candles.append([int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])])
                return candles
            else:
                logger.warning(f"OKX direct API response code: {data.get('code')}, msg: {data.get('msg')}")
    return []


async def fetch_pair_ohlcv(
    exchange_id: str,
    symbol: str,
    timeframe: str = "1m",
    limit: int = 300,
) -> list[list[Any]]:
    """Fetch OHLCV candlestick data for a single symbol from a specified exchange with retry."""
    raw_id = exchange_id.lower().strip()
    
    if raw_id == "asterdex":
        try:
            candles = await fetch_asterdex_direct_candles(symbol, timeframe, limit)
            if candles:
                return candles
        except Exception as e:
            logger.warning(f"Aster DEX direct fetch error: {e}")
            raise RuntimeError(f"Failed to fetch OHLCV from 'asterdex': {e}") from e

    # 1. OKX Direct High-Speed Handler
    if raw_id == "okx":
        try:
            candles = await fetch_okx_direct_candles(symbol, timeframe, limit)
            if candles:
                return candles
        except Exception as e:
            logger.warning(f"OKX direct fetch error: {e}, falling back to CCXT...")

    ccxt_id = EXCHANGE_MAP.get(raw_id, raw_id)
    if not hasattr(ccxt, ccxt_id):
        raise ValueError(f"Unsupported exchange ID: '{exchange_id}'")

    # Symbol normalization
    symbol_normalized = symbol.upper().strip()
    if "/" not in symbol_normalized:
        for quote in ("USDT", "USDC", "USD", "BUSD", "BTC"):
            if symbol_normalized.endswith(quote) and len(symbol_normalized) > len(quote):
                symbol_normalized = symbol_normalized[:-len(quote)] + "/" + quote
                break

    for attempt in range(2):
        exchange_class = getattr(ccxt, ccxt_id)
        exchange = exchange_class(
            {
                "enableRateLimit": True,
                "timeout": 15000,
                "options": {"defaultType": "swap"},
            }
        )

        try:
            tf_normalized = timeframe.lower().strip()
            
            # Prioritize linear swap futures format first (:USDT suffix)
            candidates = [
                f"{symbol_normalized}:USDT",
                f"{symbol_normalized}:USDC",
                symbol_normalized,
                symbol_normalized.replace("/", ""),
            ]
            
            for cand in candidates:
                try:
                    ohlcv = await exchange.fetch_ohlcv(cand, timeframe=tf_normalized, limit=limit)
                    if ohlcv:
                        return ohlcv
                except (ccxt.BadSymbol, ccxt.ExchangeError):
                    continue

            # Fallback: load markets if needed
            markets = await exchange.load_markets()
            target_symbol = symbol_normalized
            if target_symbol not in markets:
                if f"{symbol_normalized}:USDT" in markets:
                    target_symbol = f"{symbol_normalized}:USDT"
                elif f"{symbol_normalized}:USDC" in markets:
                    target_symbol = f"{symbol_normalized}:USDC"
                else:
                    base_sym = symbol_normalized.split("/")[0]
                    for m_sym, m in markets.items():
                        if m.get("swap") and m.get("base") == base_sym and m.get("settle") in ["USDT", "USDC"]:
                            target_symbol = m_sym
                            break

            ohlcv = await exchange.fetch_ohlcv(target_symbol, timeframe=tf_normalized, limit=limit)
            return ohlcv
        except Exception as e:
            if attempt == 0:
                await asyncio.sleep(0.3)
                continue
            logger.error(f"Failed fetching {symbol_normalized} on {exchange_id}: {e}")
            raise RuntimeError(f"Failed to fetch OHLCV from '{exchange_id}': {e}") from e
        finally:
            await exchange.close()

    return []


async def fetch_two_exchanges_ohlcv(
    buy_exchange_id: str,
    sell_exchange_id: str,
    symbol: str,
    timeframe: str = "1m",
    limit: int = 300,
) -> tuple[list[list[Any]], list[list[Any]]]:
    """Fetch OHLCV candles concurrently from buy and sell exchanges."""
    buy_task = fetch_pair_ohlcv(buy_exchange_id, symbol, timeframe, limit)
    sell_task = fetch_pair_ohlcv(sell_exchange_id, symbol, timeframe, limit)
    
    results = await asyncio.gather(buy_task, sell_task, return_exceptions=True)
    
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            raise res
            
    return results[0], results[1]
