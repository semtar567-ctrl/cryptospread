import asyncio
from telegram_bot.database.engine import engine
from sqlalchemy import text

async def check():
    async with engine.connect() as conn:
        print("\n--- [1] User Settings ---")
        res = await conn.execute(text("SELECT * FROM user_settings"))
        cols = res.keys()
        for row in res:
            print(dict(zip(cols, row)))

        print("\n--- [2] Enabled Exchanges ---")
        res = await conn.execute(text("SELECT * FROM user_exchanges WHERE is_enabled = true"))
        cols = res.keys()
        for row in res:
            print(dict(zip(cols, row)))
            
        print("\n--- [3] Global Config Check ---")
        res = await conn.execute(text("SELECT count(*) FROM users WHERE is_active = true"))
        print(f"Active users count: {res.scalar()}")

if __name__ == "__main__":
    asyncio.run(check())
