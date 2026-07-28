import asyncio
import ccxt.async_support as ccxt

async def check():
    okx = ccxt.okx({"options": {"defaultType": "swap"}})
    markets = await okx.load_markets()
    tickers = await okx.fetch_tickers()
    sample = ["CHZ/USDT:USDT", "ADA/USDT:USDT", "ETC/USDT:USDT", "NEAR/USDT:USDT"]
    for s in sample:
        m = markets.get(s)
        t = tickers.get(s)
        if m:
            print(f"OKX SYMBOL: {s}")
            print("  contractSize:", m.get("contractSize"))
            print("  info ctVal:", m.get("info", {}).get("ctVal"))
            print("  info ctMult:", m.get("info", {}).get("ctMult"))
            print("  info last:", t.get("info", {}).get("last") if t else None)
            print("  ticker last:", t.get("last") if t else None)
    await okx.close()

if __name__ == "__main__":
    asyncio.run(check())
