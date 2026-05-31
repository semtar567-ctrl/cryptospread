import asyncio
from redis.asyncio import Redis
import json

async def check():
    r = Redis(port=6380, decode_responses=True)
    val = await r.get('meta:okx:BTCUSDT')
    print(f"Meta for OKX BTCUSDT: {val}")
    await r.aclose()

if __name__ == "__main__":
    asyncio.run(check())
