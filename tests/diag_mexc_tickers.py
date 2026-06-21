import asyncio
import ccxt.async_support as ccxt
import json
import sys
import socket
from aiohttp.resolver import ThreadedResolver
import aiohttp

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def get_custom_session():
    connector = aiohttp.TCPConnector(resolver=ThreadedResolver(), family=socket.AF_INET)
    return aiohttp.ClientSession(connector=connector)

async def test_tickers(id, config):
    ex = getattr(ccxt, id)(config)
    ex.session = get_custom_session()
    try:
        print(f"\n--- Testing Tickers {id.upper()} ---")
        tickers = await ex.fetch_tickers()
        sample_symbol = list(tickers.keys())[0]
        print(f"Sample ticker for {sample_symbol}: {json.dumps(tickers[sample_symbol], indent=2)}")
    finally:
        await ex.close()

async def main():
    await test_tickers("mexc", {"options": {"defaultType": "swap"}})

if __name__ == "__main__":
    asyncio.run(main())
