import asyncio
import json
from redis.asyncio import Redis

async def check_redis_keys():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    
    # 1. Вайтлист
    wl = await redis.get("active_symbols_map")
    print(f"active_symbols_map: {wl[:200] if wl else 'NONE'}...")
    
    # 2. Метаданные (проверяем на примере BTC)
    meta = await redis.get("meta:bybit:BTCUSDT")
    print(f"meta:bybit:BTCUSDT: {meta}")
    
    # 3. Настройки пользователя
    user_id = "655686738"
    active = await redis.get(f"user:{user_id}:active")
    settings = await redis.get(f"user:{user_id}:settings:futures_futures")
    exchanges = await redis.get(f"user:{user_id}:exchanges")
    print(f"user:{user_id}:active: {active}")
    print(f"user:{user_id}:settings: {settings}")
    print(f"user:{user_id}:exchanges: {exchanges}")

    await redis.close()

if __name__ == "__main__":
    asyncio.run(check_redis_keys())
