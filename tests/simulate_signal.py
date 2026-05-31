import asyncio
import json
import time
from redis.asyncio import Redis
from telegram_bot.config import settings

async def simulate_custom_signal(duration=5, spread_percent=2.0, volume_usd=1000.0):
    """
    Эмуляция сигнала с заданными параметрами.
    :param duration: сколько секунд поддерживать сигнал в Redis (для проверки Lifetime)
    :param spread_percent: размер грязного спреда
    :param volume_usd: рабочий объем для расчета VWAP
    """
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    
    # Базовая цена
    base_price = 100.0
    # Цена покупки (Binance)
    buy_price = base_price
    # Цена продажи (Bybit) с учетом спреда
    sell_price = base_price * (1 + spread_percent / 100)
    
    # Эмулируем стаканы с достаточной ликвидностью для 1000$
    # На 100.0 лежит 20 монет = $2000 (хватает для $1000 без проскальзывания)
    buy_book = {
        "asks": [[buy_price, 20.0]], 
        "bids": [[buy_price - 0.1, 20.0]]
    }
    
    # На 102.0 лежит 20 монет = $2040 (хватает для $1000)
    sell_book = {
        "bids": [[sell_price, 20.0]], 
        "asks": [[sell_price + 0.1, 20.0]]
    }
    
    # Генерируем таймстамп обнаружения (имитация Rust)
    discovery_ts = int(time.time() * 1000)

    signal = {
        "symbol": "BTC/USDT",
        "buy_on": "mexc",
        "sell_on": "okx",
        "buy_price": buy_price,
        "sell_price": sell_price,
        "net_spread": spread_percent - 0.2, # Вычитаем комиссию (~0.1% на каждой стороне)
        "buy_book": buy_book,
        "sell_book": sell_book,
        "timestamp": discovery_ts
    }

    if duration >= 3:
        print(f"🚀 Запуск теста: Спред {spread_percent}%, Длительность {duration}с (Должен пройти фильтр 3с)")
    else:
        print(f"⚠️ Запуск теста: Спред {spread_percent}%, Длительность {duration}с (НЕ должен пройти фильтр 3с)")

    start_time = time.time()
    count = 0
    while time.time() - start_time < duration:
        # Обновляем таймстамп, чтобы сигнал не считался "протухшим"
        signal["timestamp"] = int(time.time() * 1000)
        await redis.xadd("market_signals", {"data": json.dumps(signal)})
        count += 1
        await asyncio.sleep(0.5) # Посылаем апдейты каждые 0.5 сек
    
    print(f"✅ Тест завершен. Отправлено {count} обновлений.")

async def main():
    # Тест: Сигнал с участием MEXC (должен прийти в бот)
    print("🚀 Запуск теста для MEXC...")
    await simulate_custom_signal(duration=5, spread_percent=3.5, volume_usd=50)

if __name__ == "__main__":
    asyncio.run(main())
