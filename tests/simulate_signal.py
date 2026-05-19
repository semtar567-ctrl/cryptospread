import asyncio
import json
from redis.asyncio import Redis
from telegram_bot.config import settings

async def simulate_market_data():
    """Скрипт для эмуляции прибыльного спреда в Redis, как если бы его нашел Rust."""
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    
    # Эмулируем стакан Binance (Покупка): Best Ask = 100
    buy_book = {
        "asks": [[100.0, 10.0], [101.0, 20.0]], # VWAP на $2000 будет ~100.33
        "bids": [[99.9, 10.0]]
    }
    
    # Эмулируем стакан Bybit (Продажа): Best Bid = 105
    sell_book = {
        "bids": [[105.0, 10.0], [104.0, 20.0]], # VWAP на $2000 будет ~104.66
        "asks": [[105.1, 10.0]]
    }
    
    # Формируем сигнал
    signal = {
        "symbol": "BTC/USDT",
        # Имена бирж должны точно совпадать с тем, что хранится в БД пользователя (без _futures)
        "buy_on": "binance",
        "sell_on": "bybit",
        "buy_price": 100.0,
        "sell_price": 105.0,
        "net_spread": 4.8, # Грубый спред
        "buy_book": buy_book,
        "sell_book": sell_book,
        "timestamp": 123456789
    }
    
    print("🚀 Отправка первого тестового сигнала в Redis (для инициализации счетчика времени жизни)...")
    await redis.xadd("market_signals", {"data": json.dumps(signal)})
    
    # Ждем 5 секунд (с запасом), чтобы гарантированно пройти фильтр в 3 секунды
    print("⏳ Ждем 5 секунд...")
    await asyncio.sleep(5)
    
    print("🚀 Отправка второго тестового сигнала в Redis (чтобы пройти фильтр времени жизни)...")
    await redis.xadd("market_signals", {"data": json.dumps(signal)})
    
    print("✅ Сигналы отправлены. Проверь Telegram!")

if __name__ == "__main__":
    asyncio.run(simulate_market_data())
