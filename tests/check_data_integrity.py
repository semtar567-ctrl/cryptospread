import asyncio
import json
from redis.asyncio import Redis

async def check_integrity():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    stream_name = "market_signals"

    print(f"--- Checking last signals from {stream_name} ---")
    try:
        # Получаем последние 5 сигналов
        messages = await redis.xrevrange(stream_name, count=5)
        
        if not messages:
            print("❌ No signals found in Redis. Make sure rust_core is running.")
            return

        for msg_id, payload in messages:
            data = json.loads(payload['data'])
            symbol = data['symbol']
            buy_ex = data['buy_on']
            sell_ex = data['sell_on']
            
            print(f"\n🚀 Signal: {symbol} | {buy_ex} -> {sell_ex} | Spread: {data['net_spread']:.3f}%")
            
            # Проверяем стакан покупки
            buy_book = data.get('buy_book', {})
            bids = buy_book.get('bids', [])
            asks = buy_book.get('asks', [])
            
            print(f"  [{buy_ex.upper()}] Depth: Bids={len(bids)}, Asks={len(asks)}")
            if asks:
                print(f"  [{buy_ex.upper()}] Top Ask: {asks[0][0]} | Vol: {asks[0][1]:.4f} (coins)")
            
            # Проверяем стакан продажи
            sell_book = data.get('sell_book', {})
            sbids = sell_book.get('bids', [])
            sasks = sell_book.get('asks', [])
            
            print(f"  [{sell_ex.upper()}] Depth: Bids={len(sbids)}, Asks={len(sasks)}")
            if sbids:
                print(f"  [{sell_ex.upper()}] Top Bid: {sbids[0][0]} | Vol: {sbids[0][1]:.4f} (coins)")

            # Валидация
            if len(asks) >= 45 and len(sbids) >= 45:
                print("  ✅ DEPTH OK (>= 45 levels)")
            else:
                print(f"  ⚠️ DEPTH LOW: {len(asks)}/{len(sbids)}")

            # Проверка объемов (на примере BTC если есть)
            if "BTC" in symbol:
                if asks and asks[0][1] > 100: # Подозрительно много для монет BTC
                    print("  ❌ VOLUME ERROR? Volume looks like contracts/USD, not coins.")
                else:
                    print("  ✅ VOLUME LOOKS REALISTIC (coins)")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await redis.close()

if __name__ == "__main__":
    asyncio.run(check_integrity())
