import asyncio
from sqlalchemy import select, update
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import User

async def force_activate():
    async with AsyncSessionLocal() as session:
        # 1. Проверяем текущий статус
        stmt = select(User)
        result = await session.execute(stmt)
        users = result.scalars().all()
        
        print("--- Before Activation ---")
        for u in users:
            print(f"User ID: {u.user_id} | Active: {u.is_active}")
            
            # 2. Принудительно ставим True
            u.is_active = True
            print(f"-> Setting User {u.user_id} to Active: True")
        
        await session.commit()
        print("\n--- DONE ---")
        print("All users are now forced to ACTIVE: True. Please restart Signal Engine.")

if __name__ == "__main__":
    asyncio.run(force_activate())
