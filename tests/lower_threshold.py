import asyncio
from sqlalchemy import update
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import UserSetting

async def lower_threshold():
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(UserSetting)
            .where(UserSetting.trade_type == 'futures_futures')
            .values(min_spread_percent=0.01)
        )
        await session.commit()
    print("Threshold lowered to 0.01%")

if __name__ == "__main__":
    asyncio.run(lower_threshold())
