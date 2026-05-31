import asyncio
import json
from redis.asyncio import Redis

async def debug_redis():
    # Мы используем порт 6380 для новой архитектуры
    redis = Redis(host='localhost', port=6380, decode_responses=True)
    
    print(f"--- CryptoSpread 2.0 Redis Debug (Port 6380) ---")
    
    # 1. Проверяем длину стрима сигналов
    try:
        stream_len = await redis.xlen("market_signals")
        print(f"Stream 'market_signals' length: {stream_len}")
    except Exception as e:
        print(f"Error reading stream: {e}")
    
    # 2. Проверяем наличие вайтлиста
    whitelist = await redis.get("active_symbols_map")
    if whitelist:
        wl_data = json.loads(whitelist)
        print(f"Whitelist 'active_symbols_map' found: {len(wl_data)} symbols")
    else:
        print(f"❌ Whitelist 'active_symbols_map' NOT FOUND in Redis port 6380!")

    # 3. Показываем последний сигнал если есть
    if 'stream_len' in locals() and stream_len > 0:
        last_sig = await redis.xrevrange("market_signals", count=1)
        print(f"Last signal: {last_sig[0][1]['data'][:200]}...")
    
    await redis.aclose()

if __name__ == "__main__":
    asyncio.run(debug_redis())
