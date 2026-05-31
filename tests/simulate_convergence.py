import asyncio
import json
import time
from redis.asyncio import Redis
from sqlalchemy import select, update
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import TrackedSpread

async def simulate_convergence():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    
    # 1. Создаем в БД тестовую запись об отслеживании
    async with AsyncSessionLocal() as session:
        # Сначала очистим старые тесты
        await session.execute(update(TrackedSpread).where(TrackedSpread.symbol == "TESTCOIN").values(is_active=False))
        
        new_track = TrackedSpread(
            user_id=123456, # Замените на ваш реальный ID для теста в телеге
            symbol="TESTCOIN",
            buy_exchange="binance",
            sell_exchange="okx",
            target_convergence=0.1, # Цель: спред 0.1% или меньше
            entry_spread=1.0,
            is_active=True
        )
        session.add(new_track)
        await session.commit()
        print("✅ Added TESTCOIN to tracked spreads (Target: 0.1%)")

    # 2. Имитируем сигнал с высоким спредом (вход)
    signal_high = {
        "symbol": "TESTCOIN",
        "buy_on": "binance",
        "sell_on": "okx",
        "buy_price": 100.0,
        "sell_price": 101.0,
        "net_spread": 0.9, # Пока выше цели
        "timestamp": int(time.time() * 1000),
        "target_users": [123456], # Ваш ID
        "user_metrics": {"123456": {"eff_net_spread": 0.9, "vwap_buy": 100, "vwap_sell": 101, "working_volume": 1000}}
    }
    
    print("🚀 Sending high spread signal...")
    await redis.xadd("market_signals", {"data": json.dumps(signal_high)})
    await asyncio.sleep(5)

    # 3. Имитируем схождение (цель достигнута)
    signal_low = {
        "symbol": "TESTCOIN",
        "buy_on": "binance",
        "sell_on": "okx",
        "buy_price": 100.0,
        "sell_price": 100.05,
        "net_spread": 0.05, # ЦЕЛЬ ДОСТИГНУТА!
        "timestamp": int(time.time() * 1000),
        "target_users": [123456],
        "user_metrics": {"123456": {"eff_net_spread": 0.05, "vwap_buy": 100, "vwap_sell": 100.05, "working_volume": 1000}}
    }
    
    print("🎯 Sending convergence signal (0.05% < 0.1%)...")
    await redis.xadd("market_signals", {"data": json.dumps(signal_low)})
    
    await redis.close()
    print("✅ Done. Check Telegram for convergence alert.")

if __name__ == "__main__":
    # ПРЕДУПРЕЖДЕНИЕ: Для реального теста вставьте свой Telegram ID
    # asyncio.run(simulate_convergence())
    print("Script prepared. Edit user_id before running.")
