import asyncio
import ccxt.async_support as ccxt
import json
import sys
import socket
from aiohttp.resolver import ThreadedResolver
from aiogram.client.session.aiohttp import AiohttpSession
import aiohttp

# Фикс для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def get_custom_session():
    connector = aiohttp.TCPConnector(resolver=ThreadedResolver(), family=socket.AF_INET)
    return aiohttp.ClientSession(connector=connector)

async def test_ex(id, config):
    ex = getattr(ccxt, id)(config)
    ex.session = get_custom_session()
    try:
        print(f"\n--- Testing {id.upper()} ---")
        markets = await ex.load_markets()
        # Find a common symbol
        test_symbol = "BTC/USDT:USDT"
        print(f"Test symbol: {test_symbol}")
        
        try:
            funding = await ex.fetch_funding_rate(test_symbol)
            print(f"Single funding rate for {test_symbol}: {json.dumps(funding, indent=2)}")
        except Exception as e:
            print(f"Error fetch_funding_rate: {e}")
            
        try:
            # Попробуем fetch_tickers для Bybit
            tickers = await ex.fetch_tickers()
            if test_symbol in tickers:
                print(f"Ticker for {test_symbol}: {json.dumps(tickers[test_symbol]['info'], indent=2)}")
            else:
                print(f"Ticker for {test_symbol} not found in {list(tickers.keys())[:5]}")
        except Exception as e:
            print(f"Error fetch_tickers: {e}")
            
    finally:
        await ex.close()

async def main():
    await test_ex("bybit", {"options": {"defaultType": "future"}})
    await test_ex("mexc", {"options": {"defaultType": "swap"}})

if __name__ == "__main__":
    asyncio.run(main())
