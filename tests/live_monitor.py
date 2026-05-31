import asyncio
import json
import time
from redis.asyncio import Redis

async def live_monitor():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    print("--- LIVE PIPELINE MONITOR ---")
    
    # 1. Проверяем настройки пользователя
    user_id = "655686738"
    is_active = await redis.get(f"user:{user_id}:active")
    settings = await redis.get(f"user:{user_id}:settings:futures_futures")
    print(f"User {user_id}: Status={'🟢 ON' if is_active=='1' else '🔴 OFF'}")
    print(f"Settings in Redis: {settings}")

    # 2. Мониторим поток сигналов в реальном времени (10 секунд)
    print("\n📡 Monitoring 'market_signals' stream for 10 seconds...")
    start_time = time.time()
    count = 0
    last_id = "$"
    
    while time.time() - start_time < 10:
        resp = await redis.xread({"market_signals": last_id}, count=10, block=500)
        if resp:
            for s_name, msgs in resp:
                for msg_id, data in msgs:
                    count += 1
                    sig = json.loads(data['data'])
                    print(f"  [LIVE] New Signal: {sig['symbol']} | {sig['buy_on']}->{sig['sell_on']} | Spread: {sig['net_spread']:.3f}%")
                    last_id = msg_id
    
    if count == 0:
        print("\n❌ NO NEW SIGNALS in 10 seconds! Rust Core is not sending anything.")
        # Проверим, шлет ли Rust вообще что-то (общая активность)
        total = await redis.xlen("market_signals")
        print(f"Total signals in Redis: {total}")
    else:
        print(f"\n✅ Received {count} new signals! Pipeline is ALIVE.")

    await redis.close()

if __name__ == "__main__":
    asyncio.run(live_monitor())
