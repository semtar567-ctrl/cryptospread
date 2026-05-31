import asyncio
import json
from redis.asyncio import Redis

async def debug_redis():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    
    # 1. Проверяем длину стрима сигналов
    stream_len = await redis.xlen("market_signals")
    print(f"--- Redis Debug ---")
    print(f"Stream 'market_signals' length: {stream_len}")
    
    # 2. Проверяем наличие вайтлиста
    whitelist = await redis.get("active_symbols_map")
    if whitelist:
        wl_data = json.loads(whitelist)
        print(f"Whitelist 'active_symbols_map' found: {len(wl_data)} symbols")
    else:
        print(f"❌ Whitelist 'active_symbols_map' NOT FOUND in Redis!")

    # 3. Показываем последний сигнал если есть
    if stream_len > 0:
        last_sig = await redis.xrevrange("market_signals", count=1)
        print(f"Last signal in Redis: {last_sig[0][1]['data'][:200]}...")
    
    await redis.close()

if __name__ == "__main__":
    asyncio.run(debug_redis())
