import asyncio
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import User, UserSetting, UserExchange

async def diag():
    async with AsyncSessionLocal() as session:
        stmt = select(User).options(
            selectinload(User.settings), 
            selectinload(User.exchanges)
        )
        users = (await session.execute(stmt)).scalars().all()
        
        print(f"--- DATABASE DIAGNOSTICS ---")
        print(f"Total users in DB: {len(users)}")
        
        for u in users:
            print(f"\nUser ID: {u.user_id} | Active: {u.is_active}")
            print(f"Exchanges:")
            for ex in u.exchanges:
                print(f"  - {ex.exchange_id}: {'ENABLED' if ex.is_enabled else 'DISABLED'}")
            
            print(f"Settings:")
            for s in u.settings:
                print(f"  - {s.trade_type}: {'ENABLED' if s.is_enabled else 'DISABLED'}")
                print(f"    Min Spread: {s.min_spread_percent}%")
                print(f"    Min Volume: {s.min_volume_24h}")
                print(f"    Working Vol: {s.working_volume_usd}")
                print(f"    Lifetime: {s.min_lifetime_seconds}s")
        print(f"\n--- END OF DIAGNOSTICS ---")

if __name__ == "__main__":
    asyncio.run(diag())
