import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

async def main():
    engine = create_async_engine(DB_URL)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE users SET is_active = true WHERE user_id = 655686738"))
        await conn.execute(text("UPDATE user_settings SET is_enabled = true WHERE user_id = 655686738"))
        # Убедимся, что биржи включены
        await conn.execute(text("UPDATE user_exchanges SET is_enabled = true WHERE user_id = 655686738"))
        print("User 655686738 activated in Postgres")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
