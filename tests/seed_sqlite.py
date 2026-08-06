import asyncio
import sys
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from telegram_bot.database.models import Base, User, UserSetting, UserExchange

async def main():
    db_url = "sqlite+aiosqlite:///./cryptospread.db"
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        user_id = 655686738
        user = await session.get(User, user_id)
        if not user:
            user = User(user_id=user_id, username="sem", is_admin=True, is_active=True)
            session.add(user)
            setting = UserSetting(
                user_id=user_id, 
                trade_type="futures_futures", 
                min_spread_percent=0.05, 
                working_volume_usd=1000.0, 
                min_volume_24h=10000.0, 
                min_lifetime_seconds=3,
                is_enabled=True,
                filter_spread_enabled=True,
                filter_volume_enabled=True,
                filter_slippage_enabled=True,
                filter_lifetime_enabled=True
            )
            session.add(setting)
            all_exchanges = [
                "binance", "bybit", "okx", "mexc", "bitget", "gateio", 
                "bingx", "kucoinfutures", "hyperliquid", "asterdex", 
                "bitmart", "htx", "dydx", "lbank", "xt"
            ]
            for ex_id in all_exchanges:
                session.add(UserExchange(user_id=user_id, exchange_id=ex_id, is_enabled=True))
            await session.commit()
            print(f"Successfully seeded real user {user_id} into SQLite cryptospread.db")
        else:
            user.is_active = True
            await session.commit()
            print(f"User {user_id} is active in SQLite")
            
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
