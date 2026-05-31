import asyncio
from telegram_bot.database.engine import engine
from sqlalchemy import text

async def migrate():
    async with engine.begin() as conn:
        print("🛠 Checking database columns...")
        # Проверяем наличие колонки (на случай если она уже есть)
        try:
            await conn.execute(text("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS filter_slippage_enabled INTEGER DEFAULT 1"))
            print("✅ Column 'filter_slippage_enabled' ensured.")
        except Exception as e:
            print(f"⚠️ Note: {e}")

        # Убеждаемся что у всех есть значения
        await conn.execute(text("UPDATE user_settings SET filter_slippage_enabled = 1 WHERE filter_slippage_enabled IS NULL"))
        print("✅ Database updated.")

if __name__ == "__main__":
    asyncio.run(migrate())
