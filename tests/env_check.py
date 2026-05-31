import asyncio
import socket
import os
import sqlite3
import json
import sys

async def check_redis(host='127.0.0.1', port=6379):
    print(f"--- 🛠 Redis Check ---")
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=3)
        print(f"✅ Redis is reachable at {host}:{port}")
        writer.close()
        await writer.wait_closed()
        return True
    except Exception as e:
        print(f"❌ Redis is NOT reachable: {e}")
        return False

def check_sqlite(db_path='cryptospread.db'):
    print(f"\n--- 🛠 SQLite Check ---")
    if not os.path.exists(db_path):
        print(f"❌ DB file NOT FOUND at {db_path}")
        return False
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"✅ DB is accessible. Tables found: {len(tables)}")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ DB access error: {e}")
        return False

async def check_network(host, port, name):
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        print(f"✅ Network: {name} is reachable")
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        print(f"❌ Network: {name} ({host}) is BLOCKED or unreachable: {e}")

async def run_checks():
    print(f"System Date: {socket.gethostname()}")
    print(f"Python Version: {sys.version}")
    
    redis_ok = await check_redis()
    sqlite_ok = check_sqlite()
    
    print(f"\n--- 🛠 Exchange Network Check ---")
    targets = [
        ("fstream.binance.com", 443, "Binance WS"),
        ("fapi.binance.com", 443, "Binance REST"),
        ("stream.bybit.com", 443, "Bybit WS"),
        ("ws.okx.com", 8443, "OKX WS"),
        ("contract.mexc.com", 443, "MEXC WS")
    ]
    await asyncio.gather(*(check_network(h, p, n) for h, p, n in targets))

    print(f"\n--- 🛠 Dependencies Check ---")
    packages = ['redis', 'sqlalchemy', 'aiogram', 'ccxt', 'structlog', 'pydantic_settings']
    for p in packages:
        try:
            __import__(p)
            print(f"✅ Package {p}: Installed")
        except ImportError:
            print(f"❌ Package {p}: MISSING")

if __name__ == "__main__":
    asyncio.run(run_checks())
