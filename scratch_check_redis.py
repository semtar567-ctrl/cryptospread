import asyncio
import json
from redis.asyncio import Redis

async def check():
    r = Redis(host='localhost', port=6380, decode_responses=True)
    data = await r.get('active_symbols_map')
    m = json.loads(data) if data else {}
    print(f"Total coins in map: {len(m)}")
    
    suspicious = []
    for unified, exs in m.items():
        for ex, info in exs.items():
            sym = info["id"].upper()
            if ex == "okx" and not "SWAP" in sym: suspicious.append((unified, ex, sym))
            if ex == "gateio" and "_" in sym:
                # Gateio futures are usually BTC_USDT (spot is also BTC_USDT)
                # But we filtered by market["swap"] in volume monitor
                pass
            
    print(f"Suspicious OKX (no SWAP): {len(suspicious)}")
    if suspicious:
        print(suspicious[:10])

    print("Sample:")
    for i, (k, v) in enumerate(m.items()):
        if i < 3:
            print(k, v)
    await r.aclose()

if __name__ == "__main__":
    asyncio.run(check())
