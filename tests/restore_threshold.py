import asyncio
from sqlalchemy import update
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import UserSetting

async def restore_threshold():
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(UserSetting)
            .where(UserSetting.trade_type == 'futures_futures')
            .values(min_spread_percent=0.5)
        )
        await session.commit()
    print("Threshold restored to 0.5%")

if __name__ == "__main__":
    asyncio.run(restore_threshold())
