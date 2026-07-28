import asyncio
import ccxt.async_support as ccxt

async def check():
    exchanges = {
        "binance": ccxt.binance({"options": {"defaultType": "future"}}),
        "bybit": ccxt.bybit({"options": {"defaultType": "future"}}),
        "okx": ccxt.okx({"options": {"defaultType": "swap"}}),
        "mexc": ccxt.mexc({"options": {"defaultType": "swap"}}),
        "bitget": ccxt.bitget({"options": {"defaultType": "swap"}}),
        "gateio": ccxt.gateio({"options": {"defaultType": "swap"}}),
        "bingx": ccxt.bingx({"options": {"defaultType": "swap"}})
    }

    try:
        for eid, ex in exchanges.items():
            markets = await ex.load_markets()
            tickers = await ex.fetch_tickers()
            for s, m in markets.items():
                if "BB" in s and s.startswith("BB/"):
                    t = tickers.get(s)
                    print(f"[{eid.upper()}] symbol: {s:25s} | id: {m.get('id'):15s} | contractSize: {m.get('contractSize')} | last: {t.get('last') if t else None} | info: {m.get('info')}")
    finally:
        for ex in exchanges.values():
            await ex.close()

if __name__ == "__main__":
    asyncio.run(check())
