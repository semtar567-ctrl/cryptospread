import asyncio
from sqlalchemy import select
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import User, UserExchange

async def repair():
    async with AsyncSessionLocal() as session:
        # Получаем всех пользователей
        stmt = select(User)
        result = await session.execute(stmt)
        users = result.scalars().all()
        
        required_exchanges = ["binance", "bybit", "okx", "mexc"]
        
        print(f"--- Repairing Database ---")
        for user in users:
            # Получаем текущие биржи пользователя
            ex_stmt = select(UserExchange).where(UserExchange.user_id == user.user_id)
            ex_res = await session.execute(ex_stmt)
            existing_ex = {ex.exchange_id for ex in ex_res.scalars().all()}
            
            for ex_id in required_exchanges:
                if ex_id not in existing_ex:
                    print(f"-> Adding {ex_id} to User {user.user_id}")
                    session.add(UserExchange(user_id=user.user_id, exchange_id=ex_id, is_enabled=True))
        
        await session.commit()
        print("--- Repair Finished ---")

if __name__ == "__main__":
    asyncio.run(repair())
