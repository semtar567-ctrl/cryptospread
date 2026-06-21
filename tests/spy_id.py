import asyncio
import os
from aiogram import Bot, Dispatcher, types
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

async def main():
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    
    @dp.message()
    async def get_id(message: types.Message):
        print(f"DEBUG: Message from {message.from_user.id} ({message.from_user.username})")
    
    print(f"SpYing on bot {TOKEN[:10]}...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
