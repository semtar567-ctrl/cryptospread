import asyncio
import ccxt.async_support as ccxt
import socket
from aiohttp.resolver import ThreadedResolver
import aiohttp

def get_custom_session():
    connector = aiohttp.TCPConnector(resolver=ThreadedResolver(), family=socket.AF_INET)
    return aiohttp.ClientSession(connector=connector)

async def check():
    ex = ccxt.bybit({'options': {'defaultType': 'future'}})
    ex.session = get_custom_session()
    tickers = await ex.fetch_tickers()
    print('Bybit USDT pairs:')
    for s, d in list(tickers.items())[:20]:
        if '/USDT' in s:
            print(f'{s}: quoteVolume={d.get("quoteVolume")} baseVolume={d.get("baseVolume")}')
    await ex.close()
    
    ex2 = ccxt.okx({'options': {'defaultType': 'swap'}})
    ex2.session = get_custom_session()
    tickers2 = await ex2.fetch_tickers()
    print('\nOKX USDT pairs:')
    for s, d in list(tickers2.items())[:20]:
        if '/USDT' in s:
            print(f'{s}: quoteVolume={d.get("quoteVolume")} baseVolume={d.get("baseVolume")}')
    await ex2.close()

if __name__ == "__main__":
    asyncio.run(check())