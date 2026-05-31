import asyncio
import json
from redis.asyncio import Redis

async def diag_user():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    
    # 1. Ищем всех пользователей в Redis
    active_keys = await redis.keys("user:*:active")
    print(f"--- Pipeline Diagnostics ---")
    
    if not active_keys:
        print("❌ No active users found in Redis! Did you press 'On' in the bot?")
        return

    for key in active_keys:
        user_id = key.split(":")[1]
        is_active = await redis.get(key)
        exchanges = await redis.get(f"user:{user_id}:exchanges")
        settings = await redis.get(f"user:{user_id}:settings:futures_futures")
        tracked = await redis.get(f"user:{user_id}:tracked")
        
        print(f"\n👤 User ID: {user_id}")
        print(f"  Status: {'🟢 ON' if is_active == '1' else '🔴 OFF'}")
        print(f"  Exchanges: {exchanges}")
        print(f"  Settings: {settings}")
        print(f"  Tracked Pairs Count: {len(json.loads(tracked)) if tracked else 0}")
        
        # Проверяем, есть ли НОВЫЕ сигналы в стриме для этого пользователя
        # (Просто смотрим последние 3 записи)
        last_signals = await redis.xrevrange("market_signals", count=3)
        if last_signals:
            data = json.loads(last_signals[0][1]['data'])
            target_users = data.get("target_users", [])
            print(f"  Latest Signal in Redis: {data['symbol']} ({data['buy_on']}->{data['sell_on']})")
            print(f"  Target Users for this signal: {target_users}")
            if int(user_id) in [int(u) for u in target_users]:
                print(f"  ✅ Rust IS SENDING signals to this user!")
            else:
                print(f"  ❌ Rust is NOT targeting this user. Reason: Spread or Volume filter.")

    await redis.close()

if __name__ == "__main__":
    asyncio.run(diag_user())
