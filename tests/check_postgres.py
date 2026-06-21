import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

async def main():
    engine = create_async_engine(DB_URL)
    async with engine.connect() as conn:
        users = await conn.execute(text("SELECT user_id, is_active FROM users"))
        print(f"Postgres Users: {users.fetchall()}")
        
        settings = await conn.execute(text("SELECT user_id, trade_type, is_enabled FROM user_settings"))
        print(f"Postgres Settings: {settings.fetchall()}")
        
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
