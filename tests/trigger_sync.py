import asyncio
from telegram_bot.utils.redis_sync import sync_all_users_to_redis
from telegram_bot.database.engine import AsyncSessionLocal

async def trigger_sync():
    async with AsyncSessionLocal() as session:
        await sync_all_users_to_redis(session)
    print("Sync triggered")

if __name__ == "__main__":
    asyncio.run(trigger_sync())
