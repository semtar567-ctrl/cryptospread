import asyncio
import aiohttp
from aiohttp.resolver import ThreadedResolver
import socket

async def main():
    print("Testing default...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.telegram.org", timeout=5) as resp:
                print("Default OK:", resp.status)
    except Exception as e:
        print("Default failed:", repr(e))

    print("\nTesting ThreadedResolver IPv4...")
    try:
        connector = aiohttp.TCPConnector(resolver=ThreadedResolver(), family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get("https://api.telegram.org", timeout=5) as resp:
                print("Threaded IPv4 OK:", resp.status)
    except Exception as e:
        print("Threaded IPv4 failed:", repr(e))

asyncio.run(main())