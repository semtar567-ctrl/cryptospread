import asyncio
from sqlalchemy import text
from telegram_bot.database.engine import engine, init_db
from telegram_bot.database.models import Base

async def setup():
    print("🚀 Initializing PostgreSQL schema...")
    try:
        # Сначала создаем таблицы через SQLAlchemy
        async with engine.begin() as conn:
            # Сброс существующих таблиц (опционально, но для чистого старта 2.0 полезно)
            # await conn.run_sync(Base.metadata.drop_all) 
            await conn.run_sync(Base.metadata.create_all)
        print("✅ Tables created successfully.")

        # Добавим начального пользователя, если база пуста (опционально)
        # async with AsyncSessionLocal() as session:
        #    ...

    except Exception as e:
        print(f"❌ Initialization failed: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(setup())
