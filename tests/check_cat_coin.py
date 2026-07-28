import asyncio
import ccxt.async_support as ccxt

async def check():
    bybit = ccxt.bybit({"options": {"defaultType": "future"}})
    bitget = ccxt.bitget({"options": {"defaultType": "swap"}})

    try:
        b_m = await bybit.load_markets()
        bg_m = await bitget.load_markets()

        b_t = await bybit.fetch_tickers()
        bg_t = await bitget.fetch_tickers()

        for s, m in b_m.items():
            if "CAT" in s:
                t = b_t.get(s)
                print(f"BYBIT: {s} | id: {m.get('id')} | last: {t.get('last') if t else None}")

        for s, m in bg_m.items():
            if "CAT" in s:
                t = bg_t.get(s)
                print(f"BITGET: {s} | id: {m.get('id')} | last: {t.get('last') if t else None}")

    finally:
        await bybit.close()
        await bitget.close()

if __name__ == "__main__":
    asyncio.run(check())
