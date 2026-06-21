import sys
import os
# Добавляем корень проекта в пути поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
import time
from redis.asyncio import Redis
from telegram_bot.config import settings

async def send_signals():
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    
    # Эталонные данные стаканов
    asks = [[100.0, 2.0], [105.0, 4.0], [110.0, 10.0]]
    bids = [[95.0, 3.0], [90.0, 5.0], [85.0, 10.0]]
    
    # 1. Формируем корректный сигнал (все расчеты идеальны)
    correct_signal = {
        "symbol": "BTC/USDT",
        "buy_on": "binance",
        "sell_on": "bybit",
        "buy_price": 100.0,
        "sell_price": 95.0,
        "buy_fee": 0.0006,
        "sell_fee": 0.0006,
        "net_spread": -5.24,
        "buy_book": {"asks": asks, "bids": []},
        "sell_book": {"asks": [], "bids": bids},
        "target_users": ["655686738"],
        "user_metrics": {
            "655686738": {
                "working_volume": 300.0,
                "vwap_buy": 101.61290322580645,
                "vwap_sell": 94.73684210526315,
                "slippage": 1.766917293233083
            }
        },
        "timestamp": int(time.time() * 1000)
    }

    # 2. Формируем некорректный сигнал (специально ломаем vwap_buy в Rust)
    incorrect_signal = {
        "symbol": "ETH/USDT",
        "buy_on": "binance",
        "sell_on": "bybit",
        "buy_price": 100.0,
        "sell_price": 95.0,
        "buy_fee": 0.0006,
        "sell_fee": 0.0006,
        "net_spread": -5.24,
        "buy_book": {"asks": asks, "bids": []},
        "sell_book": {"asks": [], "bids": bids},
        "target_users": ["655686738"],
        "user_metrics": {
            "655686738": {
                "working_volume": 300.0,
                "vwap_buy": 108.50000000000000, # СПЕЦИАЛЬНАЯ ОШИБКА: должно быть 101.6129
                "vwap_sell": 94.73684210526315,
                "slippage": 1.766917293233083
            }
        },
        "timestamp": int(time.time() * 1000)
    }

    print("[SEND] Sending CORRECT signal to Redis stream...")
    await redis.xadd("market_signals", {"data": json.dumps(correct_signal)})
    
    await asyncio.sleep(2)
    
    print("[SEND] Sending INCORRECT signal to Redis stream...")
    await redis.xadd("market_signals", {"data": json.dumps(incorrect_signal)})

    await redis.close()
    print("[OK] Testing signals sent successfully.")

if __name__ == "__main__":
    asyncio.run(send_signals())
