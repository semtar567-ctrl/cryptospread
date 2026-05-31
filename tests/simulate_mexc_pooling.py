import asyncio
import json
from redis.asyncio import Redis

async def simulate_large_whitelist():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    
    # Создаем 50 фиктивных монет для MEXC
    whitelist = {}
    for i in range(1, 51):
        symbol = f"TEST{i}USDT"
        whitelist[symbol] = {
            "mexc": {"id": f"TEST{i}_USDT", "contract_size": 1.0},
            "binance": {"id": f"TEST{i}usdt"} # Чтобы другие тоже что-то делали
        }
    
    print(f"--- Simulating whitelist with {len(whitelist)} symbols ---")
    await redis.set("active_symbols_map", json.dumps(whitelist))
    print("✅ Whitelist updated in Redis. Check rust_core logs for pooling.")
    
    await redis.close()

if __name__ == "__main__":
    asyncio.run(simulate_large_whitelist())
