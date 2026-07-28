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
        print("--- BB COIN PRICES ACROSS ALL 7 EXCHANGES ---")
        for eid, ex in exchanges.items():
            try:
                markets = await ex.load_markets()
                tickers = await ex.fetch_tickers()
                for s, m in markets.items():
                    if m.get("swap") and m.get("settle") in ["USDT", "USDC"]:
                        base = m.get("base")
                        if base == "BB" or m.get("id") in ["BBUSDT", "BB-USDT-SWAP", "BB_USDT", "BB-USDT"]:
                            t = tickers.get(s)
                            price = t.get("last") if t else None
                            fullName = m.get("info", {}).get("fullName") or m.get("info", {}).get("ctValCcy") or m.get("info", {}).get("underlyingSubType")
                            print(f"  [{eid.upper():7s}] symbol: {s:22s} | id: {m.get('id'):15s} | last_price: ${price} | meta: {fullName}")
            except Exception as err:
                print(f"  [{eid.upper():7s}] Error: {err}")
    finally:
        for ex in exchanges.values():
            await ex.close()

if __name__ == "__main__":
    asyncio.run(check())
