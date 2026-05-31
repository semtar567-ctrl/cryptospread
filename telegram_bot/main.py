import asyncio
import logging
import structlog
import aiohttp.resolver
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Фикс DNS для Windows: принудительно используем системный резолвер
aiohttp.resolver.DefaultResolver = aiohttp.resolver.ThreadedResolver

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from telegram_bot.config import settings
from telegram_bot.database.engine import init_db, AsyncSessionLocal
from telegram_bot.database.models import User, UserSetting, UserExchange
from telegram_bot.utils.keyboards import get_reply_main_menu, get_trade_types_keyboard, get_futures_settings_keyboard, get_exchanges_keyboard
from sqlalchemy import select
from sqlalchemy.orm import selectinload

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()

async def start_command(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    async with AsyncSessionLocal() as session:
        user = await session.get(User, user_id)
        if not user:
            user = User(user_id=user_id, username=username, is_admin=True)
            session.add(user)
            session.add(UserSetting(user_id=user_id, trade_type="futures_futures"))
            for ex_id in ["binance", "bybit", "okx", "mexc"]:
                session.add(UserExchange(user_id=user_id, exchange_id=ex_id))
            await session.commit()
            await message.answer(f"👋 Добро пожаловать, {username or 'друг'}!", reply_markup=get_reply_main_menu())
        else:
            await message.answer(f"👋 С возвращением!", reply_markup=get_reply_main_menu())

# Хендлеры нижней панели (Reply Keyboard)
async def menu_button_handler(message: types.Message):
    await message.answer("⚠️ Фильтры: каждый тип связок имеет собственный набор уникальных фильтров", 
                         reply_markup=get_trade_types_keyboard())

async def profile_button_handler(message: types.Message):
    await message.answer(f"👤 Профиль\nID: {message.from_user.id}\nUsername: @{message.from_user.username}")

# Обработка Inline кнопок
async def main_menu_callback(callback: types.CallbackQuery):
    await callback.message.edit_text("⚠️ Фильтры: каждый тип связок имеет собственный набор уникальных фильтров", 
                                     reply_markup=get_trade_types_keyboard())

async def futures_settings_callback(callback: types.CallbackQuery):
    async with AsyncSessionLocal() as session:
        stmt = select(UserSetting).where(UserSetting.user_id == callback.from_user.id, UserSetting.trade_type == "futures_futures")
        result = await session.execute(stmt)
        settings_obj = result.scalar_one_or_none()
        
        if settings_obj:
            await callback.message.edit_text("⚙️ Настройки Фьючерс/Фьючерс:", 
                                             reply_markup=get_futures_settings_keyboard(settings_obj))
        else:
            await callback.answer("Ошибка: настройки не найдены", show_alert=True)

async def exchanges_settings_callback(callback: types.CallbackQuery):
    async with AsyncSessionLocal() as session:
        # Проверяем и добавляем недостающие биржи для пользователя
        user_id = callback.from_user.id
        stmt = select(UserExchange).where(UserExchange.user_id == user_id)
        result = await session.execute(stmt)
        existing_exchanges = {ex.exchange_id for ex in result.scalars().all()}
        
        required_exchanges = ["binance", "bybit", "okx", "mexc"]
        added = False
        for ex_id in required_exchanges:
            if ex_id not in existing_exchanges:
                session.add(UserExchange(user_id=user_id, exchange_id=ex_id))
                added = True
        
        if added:
            await session.commit()

        # Получаем актуальный список
        stmt = select(UserExchange).where(UserExchange.user_id == user_id)
        result = await session.execute(stmt)
        user_exchanges = result.scalars().all()
        
        await callback.message.edit_text("🏛 Выберите биржи для мониторинга:", 
                                         reply_markup=get_exchanges_keyboard(user_exchanges))

# Логика переключения фильтров (ON/OFF)
async def toggle_setting_callback(callback: types.CallbackQuery):
    field_map = {
        "toggle_futures_enabled": "is_enabled",
        "toggle_filter_volume": "filter_volume_enabled",
        "toggle_filter_slippage": "filter_slippage_enabled",
        "toggle_filter_lifetime": "filter_lifetime_enabled",
    }
    field = field_map.get(callback.data)
    if not field: return

    async with AsyncSessionLocal() as session:
        stmt = select(UserSetting).where(UserSetting.user_id == callback.from_user.id, UserSetting.trade_type == "futures_futures")
        result = await session.execute(stmt)
        settings_obj = result.scalar_one_or_none()
        
        if settings_obj:
            current_val = getattr(settings_obj, field)
            setattr(settings_obj, field, not current_val)
            await session.commit()
            
            # СИНХРОНИЗАЦИЯ С REDIS
            await sync_user_settings_to_redis(callback.from_user.id, session)
            
            # Обновляем меню
            await callback.message.edit_reply_markup(reply_markup=get_futures_settings_keyboard(settings_obj))
            await callback.answer("Настройка сохранена")

# Логика переключения бирж
async def toggle_exchange_callback(callback: types.CallbackQuery):
    ex_id = callback.data.replace("toggle_ex_", "")
    
    async with AsyncSessionLocal() as session:
        stmt = select(UserExchange).where(UserExchange.user_id == callback.from_user.id, UserExchange.exchange_id == ex_id)
        result = await session.execute(stmt)
        ex_obj = result.scalar_one_or_none()
        
        if ex_obj:
            ex_obj.is_enabled = not ex_obj.is_enabled
            await session.commit()
            
            # СИНХРОНИЗАЦИЯ С REDIS
            await sync_user_settings_to_redis(callback.from_user.id, session)
            
            # Получаем обновленный список для перерисовки
            stmt_all = select(UserExchange).where(UserExchange.user_id == callback.from_user.id)
            result_all = await session.execute(stmt_all)
            user_exchanges = result_all.scalars().all()
            
            await callback.message.edit_reply_markup(reply_markup=get_exchanges_keyboard(user_exchanges))
            await callback.answer(f"Биржа {ex_id} {'включена' if ex_obj.is_enabled else 'выключена'}")

async def close_menu_callback(callback: types.CallbackQuery):
    await callback.message.delete()

async def placeholder_callback(callback: types.CallbackQuery):
    await callback.answer("🚧 Этот раздел находится в разработке", show_alert=True)

from aiogram.fsm.context import FSMContext
from telegram_bot.utils.states import SettingsStates

from telegram_bot.utils.redis_sync import sync_user_settings_to_redis, sync_all_users_to_redis

# ... (start_command, menu_button_handler, profile_button_handler без изменений)

# Хендлеры начала редактирования (FSM)
async def edit_spread_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(SettingsStates.wait_for_spread)
    await callback.message.answer("Введите минимальный спред в % (например, 0.5):")
    await callback.answer()

async def edit_work_vol_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(SettingsStates.wait_for_work_vol)
    await callback.message.answer("Введите ваш рабочий объем в USD (например, 1000):")
    await callback.answer()

async def edit_volume_24h_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(SettingsStates.wait_for_volume_24h)
    await callback.message.answer("Введите минимальный суточный объем в миллионах USD (например, 10):")
    await callback.answer()

async def edit_lifetime_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(SettingsStates.wait_for_lifetime)
    await callback.message.answer("Введите время жизни связки в секундах (например, 5):")
    await callback.answer()

# Хендлеры обработки введенных чисел
async def process_numeric_input(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    try:
        val = float(message.text.replace(",", "."))
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число.")
        return

    field_map = {
        SettingsStates.wait_for_spread.state: "min_spread_percent",
        SettingsStates.wait_for_work_vol.state: "working_volume_usd",
        SettingsStates.wait_for_volume_24h.state: "min_volume_24h",
        SettingsStates.wait_for_lifetime.state: "min_lifetime_seconds",
    }
    
    field = field_map.get(current_state)
    if field == "min_lifetime_seconds":
        val = int(val)

    async with AsyncSessionLocal() as session:
        stmt = select(UserSetting).where(UserSetting.user_id == message.from_user.id, UserSetting.trade_type == "futures_futures")
        result = await session.execute(stmt)
        settings_obj = result.scalar_one_or_none()
        
        if settings_obj:
            setattr(settings_obj, field, val)
            await session.commit()
            
            # СИНХРОНИЗАЦИЯ С REDIS
            await sync_user_settings_to_redis(message.from_user.id, session)
            
            await message.answer(f"✅ Значение сохранено!", reply_markup=get_futures_settings_keyboard(settings_obj))
            await state.clear()

# Логика главной кнопки ON/OFF (уведомления)
async def toggle_notifications_handler(message: types.Message):
    async with AsyncSessionLocal() as session:
        user = await session.get(User, message.from_user.id)
        if user:
            user.is_active = not user.is_active
            await session.commit()
            
            # СИНХРОНИЗАЦИЯ С REDIS
            await sync_user_settings_to_redis(message.from_user.id, session)
            
            status_text = "включены" if user.is_active else "выключены"
            await message.answer(f"🔔 Уведомления {status_text}", reply_markup=get_reply_main_menu(user.is_active))

# ... (main_menu_callback, futures_settings_callback, exchanges_settings_callback и др.)

import socket
from aiogram.client.session.aiohttp import AiohttpSession
from aiohttp.resolver import ThreadedResolver
from aiogram.client.default import DefaultBotProperties

class IPv4Session(AiohttpSession):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Фикс для Windows + VPN: принудительно используем IPv4 и стандартный резолвер
        self._connector_init["resolver"] = ThreadedResolver()
        self._connector_init["family"] = socket.AF_INET

async def main():
    logger.info("Initializing database...")
    await init_db()
    
    # ПЕРВИЧНАЯ СИНХРОНИЗАЦИЯ ВСЕХ ПОЛЬЗОВАТЕЛЕЙ В REDIS
    logger.info("Syncing all users to Redis...")
    async with AsyncSessionLocal() as session:
        await sync_all_users_to_redis(session)
    
    logger.info("Starting Telegram Bot...")
    
    bot = Bot(
        token=settings.telegram_bot_token, 
        session=IPv4Session(),
        default=DefaultBotProperties(parse_mode="HTML")
    )
    dp = Dispatcher()
    # Регистрация команд
    dp.message.register(start_command, Command("start"))
    
    # Регистрация Reply кнопок
    dp.message.register(menu_button_handler, F.text == "⚙️ Меню")
    dp.message.register(profile_button_handler, F.text == "👤 Профиль")
    dp.message.register(toggle_notifications_handler, (F.text == "🟢 On") | (F.text == "🔴 Off"))

    # Регистрация FSM хендлеров (ввод чисел)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_spread)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_work_vol)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_volume_24h)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_lifetime)

    # Регистрация Callback (Инлайн кнопки)
    dp.callback_query.register(main_menu_callback, F.data == "main_menu")
    dp.callback_query.register(futures_settings_callback, F.data == "settings_futures_futures")
    dp.callback_query.register(exchanges_settings_callback, F.data == "settings_exchanges")
    
    # Вызов редактирования (FSM)
    dp.callback_query.register(edit_spread_callback, F.data == "edit_spread")
    dp.callback_query.register(edit_work_vol_callback, F.data == "edit_work_vol")
    dp.callback_query.register(edit_volume_24h_callback, F.data == "edit_volume")
    dp.callback_query.register(edit_lifetime_callback, F.data == "edit_lifetime")
    
    # Регистрация переключателей (ON/OFF)
    dp.callback_query.register(toggle_setting_callback, F.data.startswith("toggle_filter_") | (F.data == "toggle_futures_enabled"))
    dp.callback_query.register(toggle_exchange_callback, F.data.startswith("toggle_ex_"))
    
    dp.callback_query.register(close_menu_callback, F.data == "close_menu")
    
    # Заглушки для типов торговли
    dp.callback_query.register(placeholder_callback, F.data.startswith("trade_"))
    dp.callback_query.register(placeholder_callback, F.data == "hint")

    logger.info("Starting Telegram Bot...")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
