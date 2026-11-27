"""FastAPI Web Server for CryptoSpread Dashboard & Chart.

Serves REST endpoints for spread candles, full symbol list, and WebSocket live-spread stream.
"""

import asyncio
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

# Добавляем корень проекта в sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import redis.asyncio as aioredis
import uvicorn

from telegram_bot.config import settings
from web.spread_engine.calculator import calculate_ratio_candles
from web.spread_engine.fetcher import fetch_two_exchanges_ohlcv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("web.server")

app = FastAPI(
    title="CryptoSpread Web Terminal & Chart API",
    version="1.1.0",
    description="Live Spread Terminal & TradingView Lightweight Charts API with Realtime WebSockets",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared Redis connection
_redis_pool = None

async def get_redis():
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = aioredis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True
        )
    return _redis_pool


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "app": "cryptospread-web"}


@app.get("/api/v1/symbols")
async def get_symbols() -> Dict[str, Any]:
    """Returns list of all active normalized symbols across 15 exchanges."""
    r = await get_redis()
    try:
        raw_map = await r.get("active_symbols_map")
        if raw_map:
            data = json.loads(raw_map)
            # Format symbols with slash (e.g. BTCUSDT -> BTC/USDT)
            formatted = []
            for sym in data.keys():
                clean = sym.upper()
                if "/" in clean:
                    formatted.append(clean)
                else:
                    for quote in ("USDT", "USDC", "USD", "BUSD"):
                        if clean.endswith(quote) and len(clean) > len(quote):
                            clean = clean[:-len(quote)] + "/" + quote
                            break
                    formatted.append(clean)
            formatted = sorted(list(set(formatted)))
            return {"success": True, "count": len(formatted), "symbols": formatted}
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")

    # Fallback to top pairs if Redis is warming up
    fallback = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "DOGE/USDT", "ONE/USDT", "COTI/USDT", "COW/USDT", "ETHFI/USDT", "PEPE/USDT", "SUI/USDT", "NEAR/USDT"]
    return {"success": True, "count": len(fallback), "symbols": sorted(fallback)}


@app.get("/api/v1/spread-candles")
async def get_spread_candles(
    symbol: str = Query("BTC/USDT", description="Trading pair symbol (e.g. BTC/USDT or BTCUSDT)"),
    buy_exchange: str = Query("binance", description="Buy Exchange ID"),
    sell_exchange: str = Query("bybit", description="Sell Exchange ID"),
    timeframe: str = Query("1m", description="Timeframe (1m, 5m, 15m, 1h, 4h, 1D)"),
    limit: int = Query(300, ge=10, le=1000, description="Candle limit (10..1000)"),
) -> Dict[str, Any]:
    """Calculates ratio spread candles from two exchanges concurrently."""
    try:
        buy_ohlcv, sell_ohlcv = await fetch_two_exchanges_ohlcv(
            buy_exchange_id=buy_exchange,
            sell_exchange_id=sell_exchange,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )
        candles = await asyncio.to_thread(calculate_ratio_candles, buy_ohlcv, sell_ohlcv)
        return {
            "success": True,
            "symbol": symbol,
            "buy_exchange": buy_exchange,
            "sell_exchange": sell_exchange,
            "timeframe": timeframe,
            "count": len(candles),
            "candles": candles,
        }
    except ValueError as e:
        logger.warning(f"Validation error in spread-candles: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in spread-candles endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


EXCHANGE_NORM = {
    "gate": "gateio",
    "kucoin": "kucoinfutures",
    "coinex": "coinexfutures",
    "aster": "asterdex",
}


@app.websocket("/ws/live-spread")
async def websocket_live_spread(
    websocket: WebSocket,
    buy_exchange: str = "binance",
    sell_exchange: str = "bybit",
    symbol: str = "BTC/USDT"
):
    """Streams live real-time spread percent ticks for the active pair directly from Redis."""
    await websocket.accept()
    r = await get_redis()
    
    clean_sym = symbol.replace("/", "").replace("-", "").upper()
    raw_buy = buy_exchange.lower().strip()
    raw_sell = sell_exchange.lower().strip()
    buy_ex = EXCHANGE_NORM.get(raw_buy, raw_buy)
    sell_ex = EXCHANGE_NORM.get(raw_sell, raw_sell)
    
    logger.info(f"WebSocket client connected for {clean_sym} ({buy_ex} -> {sell_ex})")
    
    try:
        while True:
            # 1. Try to read directly from active spread key (calculated by rust_core)
            spread_key = f"current_spread:futures_futures:{clean_sym}:{buy_ex}:{sell_ex}"
            raw_spread = await r.get(spread_key)
            
            now_sec = int(time.time())
            sent = False
            if raw_spread:
                s_data = json.loads(raw_spread)
                buy_p = s_data.get("buy_price", 0.0)
                sell_p = s_data.get("sell_price", 0.0)
                if buy_p > 0 and sell_p > 0:
                    spread_pct = s_data.get("vwap_net_spread", ((sell_p / buy_p) - 1.0) * 100.0)
                    
                    await websocket.send_json({
                        "time": now_sec,
                        "spread_percent": round(spread_pct, 4),
                        "buy_price": buy_p,
                        "sell_price": sell_p,
                        "is_live": True
                    })
                    sent = True
            
            if not sent:
                # 2. Fallback: check metadata prices directly
                b_meta = await r.get(f"meta:{buy_ex}:{clean_sym}")
                s_meta = await r.get(f"meta:{sell_ex}:{clean_sym}")
                if b_meta and s_meta:
                    bm = json.loads(b_meta)
                    sm = json.loads(s_meta)
                    buy_p = bm.get("last_price", 0.0) or bm.get("ask_price", 0.0)
                    sell_p = sm.get("last_price", 0.0) or sm.get("bid_price", 0.0)
                    if buy_p > 0 and sell_p > 0:
                        spread_pct = ((sell_p / buy_p) - 1.0) * 100.0
                        await websocket.send_json({
                            "time": now_sec,
                            "spread_percent": round(spread_pct, 4),
                            "buy_price": buy_p,
                            "sell_price": sell_p,
                            "is_live": True
                        })

            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected for {clean_sym}")
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
        try:
            await websocket.close()
        except: pass


# Mount static files (index.html, app.js, style.css, lightweight-charts.js)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("web.server:app", host="0.0.0.0", port=8000, reload=False)
