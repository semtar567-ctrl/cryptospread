import asyncio
import time
import json
import logging
import structlog
import aiohttp.resolver
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiogram.fsm.context import FSMContext
from telegram_bot.utils.states import SettingsStates

# Фикс DNS для Windows: принудительно используем системный резолвер
aiohttp.resolver.DefaultResolver = aiohttp.resolver.ThreadedResolver

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from telegram_bot.config import settings
from telegram_bot.database.engine import init_db, AsyncSessionLocal
from telegram_bot.database.models import User, UserSetting, UserExchange, TrackedSpread
from telegram_bot.utils.keyboards import get_reply_main_menu, get_trade_types_keyboard, get_futures_settings_keyboard, get_exchanges_keyboard
from sqlalchemy import select
from sqlalchemy.orm import selectinload

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()

async def spreads_button_handler(message: types.Message):
    async with AsyncSessionLocal() as session:
        stmt = select(TrackedSpread).where(TrackedSpread.user_id == message.from_user.id, TrackedSpread.is_active == True)
        result = await session.execute(stmt)
        tracked_count = len(result.scalars().all())
        
        await message.answer(
            "⚠️ **СВЯЗКИ**\n\nЗдесь можно посмотреть актуальные связки по выбранному типу сигналов.",
            parse_mode="Markdown",
            reply_markup=get_trade_types_keyboard(tracked_count)
        )

async def show_categories_callback(callback: types.CallbackQuery):
    async with AsyncSessionLocal() as session:
        stmt = select(TrackedSpread).where(TrackedSpread.user_id == callback.from_user.id, TrackedSpread.is_active == True)
        result = await session.execute(stmt)
        tracked_count = len(result.scalars().all())
        
        await callback.message.edit_text(
            "⚠️ **СВЯЗКИ**\n\nЗдесь можно посмотреть актуальные связки по выбранному типу сигналов.",
            parse_mode="Markdown",
            reply_markup=get_trade_types_keyboard(tracked_count)
        )

async def category_futures_futures_callback(callback: types.CallbackQuery):
    from redis.asyncio import Redis
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    try:
        keys = await redis.keys("current_spread:futures_futures:*")
        spreads = []
        for key in keys:
            data = await redis.get(key)
            if data:
                spreads.append(json.loads(data))
        
        spreads.sort(key=lambda s: s.get("vwap_net_spread", 0.0), reverse=True)
        
        text = "🟢 **СВЯЗКИ: ФЬЮЧЕРС/ФЬЮЧЕРС МЕЖБИРЖ**\n\nВыберите связку для просмотра подробной информации:\n\n"
        buttons = []
        
        if not spreads:
            text += "*(нет активных связок в данный момент)*"
        else:
            for i, s in enumerate(spreads[:10], 1):
                symbol = s['symbol']
                buy_ex = s['buy_ex']
                sell_ex = s['sell_ex']
                spread_val = s['vwap_net_spread']
                text += f"{i}. {symbol} ({buy_ex.upper()} -> {sell_ex.upper()}) | **{spread_val:.2f}%**\n"
                
                btn_text = f"{symbol} ({buy_ex.upper()}->{sell_ex.upper()}) | {spread_val:.2f}%"
                buttons.append([types.InlineKeyboardButton(
                    text=btn_text, 
                    callback_data=f"show_spread_futures_futures_{symbol}_{buy_ex}_{sell_ex}"
                )])
        
        buttons.append([
            types.InlineKeyboardButton(text="🔄 Обновить", callback_data="category_futures_futures"),
            types.InlineKeyboardButton(text="⬅️ Назад", callback_data="show_categories")
        ])
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    finally:
        await redis.aclose()

async def show_spread_details_callback(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) < 7:
        await callback.answer("Ошибка формата данных", show_alert=True)
        return
    
    symbol = parts[4]
    buy_ex = parts[5]
    sell_ex = parts[6]
    
    from redis.asyncio import Redis
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    try:
        key = f"current_spread:futures_futures:{symbol}:{buy_ex}:{sell_ex}"
        data_raw = await redis.get(key)
        if not data_raw:
            await callback.answer("⚠️ Эта связка больше не активна (спред ушел).", show_alert=True)
            await category_futures_futures_callback(callback)
            return
        
        s = json.loads(data_raw)
        
        tv_symbol = f"{s['buy_ex'].upper()}:{s['symbol']}.P/{s['sell_ex'].upper()}:{s['symbol']}.P"
        tv_link = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
        
        warn_b = " ⚠️ Низкая ликвидность" if not s.get("buy_full", True) else ""
        warn_s = " ⚠️ Низкая ликвидность" if not s.get("sell_full", True) else ""
        
        def format_time_to_funding(ts_ms):
            if not ts_ms:
                from datetime import datetime, timedelta, timezone
                now_utc = datetime.now(timezone.utc)
                target_hour = (now_utc.hour // 8 + 1) * 8
                if target_hour >= 24:
                    target_dt = now_utc.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                else:
                    target_dt = now_utc.replace(hour=target_hour, minute=0, second=0, microsecond=0)
                ts_sec = target_dt.timestamp()
            else:
                ts_sec = int(ts_ms) / 1000
            try:
                diff = ts_sec - time.time()
                from datetime import datetime, timedelta, timezone
                dt_msk = datetime.fromtimestamp(ts_sec, timezone(timedelta(hours=3)))
                time_str = dt_msk.strftime("%H:%M")
                if diff < 0: return f"{time_str} (сейчас)"
                hours = int(diff // 3600)
                minutes = int((diff % 3600) // 60)
                return f"{time_str} (через {hours}ч {minutes}м)"
            except: return "н/д"

        msg = (
            f"🟦🟧 **{s['symbol']}** (F/F)\n"
            f"↑ {s['buy_ex'].upper()} F. / ↓ {s['sell_ex'].upper()} F.\n\n"
            f"🛠 **Комиссии:** {s['total_fee']:.3f}%\n"
            f"📊 **Курсовой Спред:** {s['net_spread']:.2f}%\n"
            f"💎 **Фандинг Спред:** {s['funding_spread']:.3f}%\n"
            f"💰 **Объем:** ${s['vol_usd']:g}\n"
            f"🚀 **Спред (VWAP):** **{s['vwap_net_spread']:.2f}%**\n\n"
            f"🟩 **ЛОНГ НА ФЬЮЧЕРСЕ:**\n"
            f"[{s['buy_ex'].upper()}]({_get_ex_link_main(s['buy_ex'], s['symbol'])})\n"
            f"Цена: {s['buy_price']:.5f}\n"
            f"Фандинг: {s['buy_funding']:.3f}% | {format_time_to_funding(s['buy_next_f'])}\n"
            f"Объем 24ч: ${s['buy_vol_24h']/1e6:.1f}M\n"
            f"Проскальзывание: {s['buy_slip']:.3f}%{warn_b}\n\n"
            f"🟥 **ШОРТ НА ФЬЮЧЕРСЕ:**\n"
            f"[{s['sell_ex'].upper()}]({_get_ex_link_main(s['sell_ex'], s['symbol'])})\n"
            f"Цена: {s['sell_price']:.5f}\n"
            f"Фандинг: {s['sell_funding']:.3f}% | {format_time_to_funding(s['sell_next_f'])}\n"
            f"Объем 24ч: ${s['sell_vol_24h']/1e6:.1f}M\n"
            f"Проскальзывание: {s['sell_slip']:.3f}%{warn_s}\n\n"
            f"📈 [График спреда]({tv_link})"
        )
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🎯 Отслеживать схождение", callback_data=f"select_conv_{symbol}_{buy_ex}_{sell_ex}")],
            [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="category_futures_futures")]
        ])
        
        await callback.message.edit_text(msg, parse_mode="Markdown", reply_markup=kb, disable_web_page_preview=True)
    finally:
        await redis.aclose()

def _get_ex_link_main(exchange, symbol):
    return {
        "binance": f"https://www.binance.com/en/futures/{symbol}",
        "bybit": f"https://www.bybit.com/trade/usdt/{symbol}",
        "okx": f"https://www.okx.com/trade-swap/{symbol.lower()}-usdt",
        "mexc": f"https://contract.mexc.com/exchange/{symbol}_USDT"
    }.get(exchange, "#")

async def select_convergence_callback(callback: types.CallbackQuery):
    data = callback.data
    prefix = "select_conv_" if data.startswith("select_conv_") else "track_"
    parts = data.replace(prefix, "").split("_")
    
    if len(parts) < 3:
        await callback.answer("Ошибка формата данных", show_alert=True)
        return
    
    symbol = parts[0]
    buy_ex = parts[1]
    sell_ex = parts[2]
    
    text = (
        f"🎯 **ОТСЛЕЖИВАНИЕ СХОЖДЕНИЯ**\n\n"
        f"Вы настраиваете схождение для пары:\n"
        f"**{symbol}** ({buy_ex.upper()} -> {sell_ex.upper()})\n\n"
        f"Выберите желаемый порог спреда, при достижении которого бот пришлет уведомление:"
    )
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [
            types.InlineKeyboardButton(text="0.75%", callback_data=f"set_conv_{symbol}_{buy_ex}_{sell_ex}_0.75"),
            types.InlineKeyboardButton(text="0.50%", callback_data=f"set_conv_{symbol}_{buy_ex}_{sell_ex}_0.5"),
            types.InlineKeyboardButton(text="0.25%", callback_data=f"set_conv_{symbol}_{buy_ex}_{sell_ex}_0.25")
        ],
        [types.InlineKeyboardButton(text="⌨️ Ввести вручную", callback_data=f"set_conv_manual_{symbol}_{buy_ex}_{sell_ex}")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"show_spread_futures_futures_{symbol}_{buy_ex}_{sell_ex}")]
    ])
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)

async def set_convergence_val_callback(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) < 6:
        await callback.answer("Ошибка формата", show_alert=True)
        return
    
    symbol = parts[2]
    buy_ex = parts[3]
    sell_ex = parts[4]
    val = float(parts[5])
    
    async with AsyncSessionLocal() as session:
        stmt = select(TrackedSpread).where(
            TrackedSpread.user_id == callback.from_user.id,
            TrackedSpread.symbol == symbol,
            TrackedSpread.buy_exchange == buy_ex,
            TrackedSpread.sell_exchange == sell_ex
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            existing.target_convergence = val
            existing.is_active = True
        else:
            new_track = TrackedSpread(
                user_id=callback.from_user.id,
                symbol=symbol,
                buy_exchange=buy_ex,
                sell_exchange=sell_ex,
                entry_spread=0.0,
                target_convergence=val,
                is_active=True
            )
            session.add(new_track)
        
        await session.commit()
        
        from telegram_bot.utils.redis_sync import sync_user_settings_to_redis
        await sync_user_settings_to_redis(callback.from_user.id, session)
        
        await callback.answer(f"🚀 Начал отслеживание {symbol}!\nУведомление придет при сходе до {val}%.", show_alert=True)
        
        callback.data = f"show_spread_futures_futures_{symbol}_{buy_ex}_{sell_ex}"
        await show_spread_details_callback(callback)

async def set_convergence_manual_callback(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 6:
        await callback.answer("Ошибка формата", show_alert=True)
        return
    
    symbol = parts[3]
    buy_ex = parts[4]
    sell_ex = parts[5]
    
    await state.set_state(SettingsStates.wait_for_convergence)
    await state.update_data(symbol=symbol, buy_ex=buy_ex, sell_ex=sell_ex)
    
    await callback.message.answer(
        f"⌨️ **РУЧНОЙ ВВОД ПОРОГА СХОЖДЕНИЯ**\n\n"
        f"Для пары: {symbol} ({buy_ex.upper()} -> {sell_ex.upper()})\n"
        f"Введите желаемый процент схождения (например, 0.15):"
    )
    await callback.answer()

async def show_tracked_list_callback(callback: types.CallbackQuery):
    async with AsyncSessionLocal() as session:
        stmt = select(TrackedSpread).where(TrackedSpread.user_id == callback.from_user.id, TrackedSpread.is_active == True)
        result = await session.execute(stmt)
        tracked = result.scalars().all()
        
        if not tracked:
            await callback.answer("📍 У вас нет активных отслеживаемых связок.", show_alert=True)
            return
        
        text = "📍 **ВАШИ ОТСЛЕЖИВАЕМЫЕ СВЯЗКИ**\n\nВыберите связку для управления:\n"
        buttons = []
        
        for i, t in enumerate(tracked, 1):
            text += f"{i}. {t.symbol} ({t.buy_exchange.upper()} -> {t.sell_exchange.upper()}) | Цель: **{t.target_convergence}%**\n"
            buttons.append([types.InlineKeyboardButton(
                text=f"{t.symbol} ({t.buy_exchange.upper()}->{t.sell_exchange.upper()})", 
                callback_data=f"show_tracked_details_{t.id}"
            )])
            
        buttons.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="show_categories")])
        
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=buttons))

async def show_tracked_details_callback(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) < 4:
        await callback.answer("Ошибка формата", show_alert=True)
        return
    
    t_id = int(parts[3])
    
    async with AsyncSessionLocal() as session:
        record = await session.get(TrackedSpread, t_id)
        if not record or not record.is_active:
            await callback.answer("Связка больше не активна или удалена.", show_alert=True)
            await show_tracked_list_callback(callback)
            return
        
        from redis.asyncio import Redis
        redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
        current_spread_val = "н/д"
        try:
            key = f"current_spread:futures_futures:{record.symbol}:{record.buy_exchange}:{record.sell_exchange}"
            data_raw = await redis.get(key)
            if data_raw:
                s = json.loads(data_raw)
                current_spread_val = f"{s['vwap_net_spread']:.2f}%"
        finally:
            await redis.aclose()
            
        text = (
            f"📍 **ОТСЛЕЖИВАЕМАЯ СВЯЗКА**\n\n"
            f"🪙 **Символ:** {record.symbol}\n"
            f"🟢 Покупка: {record.buy_exchange.upper()}\n"
            f"🟥 Продажа: {record.sell_exchange.upper()}\n"
            f"🎯 Целевое схождение: **{record.target_convergence}%**\n"
            f"📊 Текущий спред: **{current_spread_val}**\n\n"
            f"Бот пришлет уведомление, как только текущий спред станет меньше или равен цели."
        )
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🎯 Изменить схождение", callback_data=f"select_conv_{record.symbol}_{record.buy_exchange}_{record.sell_exchange}")],
            [types.InlineKeyboardButton(text="❌ Удалить из отслеживания", callback_data=f"untrack_spread_{record.id}")],
            [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="show_tracked_list")]
        ])
        
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)

async def untrack_spread_callback(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.answer("Ошибка формата", show_alert=True)
        return
    
    t_id = int(parts[2])
    
    async with AsyncSessionLocal() as session:
        record = await session.get(TrackedSpread, t_id)
        if record:
            record.is_active = False
            await session.commit()
            
            from telegram_bot.utils.redis_sync import sync_user_settings_to_redis
            await sync_user_settings_to_redis(callback.from_user.id, session)
            
            await callback.answer("🗑 Связка удалена из отслеживания.", show_alert=True)
        
        stmt = select(TrackedSpread).where(TrackedSpread.user_id == callback.from_user.id, TrackedSpread.is_active == True)
        result = await session.execute(stmt)
        tracked = result.scalars().all()
        if tracked:
            await show_tracked_list_callback(callback)
        else:
            await show_categories_callback(callback)

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

# FSM imports moved to top

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

    # Отдельно обрабатываем ввод порога схождения
    if current_state == SettingsStates.wait_for_convergence.state:
        data = await state.get_data()
        symbol = data["symbol"]
        buy_ex = data["buy_ex"]
        sell_ex = data["sell_ex"]
        
        async with AsyncSessionLocal() as session:
            stmt = select(TrackedSpread).where(
                TrackedSpread.user_id == message.from_user.id,
                TrackedSpread.symbol == symbol,
                TrackedSpread.buy_exchange == buy_ex,
                TrackedSpread.sell_exchange == sell_ex
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            
            if existing:
                existing.target_convergence = val
                existing.is_active = True
            else:
                new_track = TrackedSpread(
                    user_id=message.from_user.id,
                    symbol=symbol,
                    buy_exchange=buy_ex,
                    sell_exchange=sell_ex,
                    entry_spread=0.0,
                    target_convergence=val,
                    is_active=True
                )
                session.add(new_track)
            
            await session.commit()
            
            from telegram_bot.utils.redis_sync import sync_user_settings_to_redis
            await sync_user_settings_to_redis(message.from_user.id, session)
            
            # Считаем количество активных отслеживаемых
            stmt_count = select(TrackedSpread).where(TrackedSpread.user_id == message.from_user.id, TrackedSpread.is_active == True)
            res_count = await session.execute(stmt_count)
            tracked_count = len(res_count.scalars().all())
            
            await message.answer(
                f"🚀 Успешно! Установлен порог схождения {val}% для {symbol}.\n"
                f"Я пришлю уведомление, когда спред сойдется.",
                reply_markup=get_trade_types_keyboard(tracked_count)
            )
            await state.clear()
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
    dp.message.register(spreads_button_handler, F.text == "🔔 Связки")
    dp.message.register(toggle_notifications_handler, (F.text == "🟢 On") | (F.text == "🔴 Off"))

    # Регистрация FSM хендлеров (ввод чисел)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_spread)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_work_vol)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_volume_24h)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_lifetime)
    dp.message.register(process_numeric_input, SettingsStates.wait_for_convergence)

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
    
    # Связки и схождение
    dp.callback_query.register(show_categories_callback, F.data == "show_categories")
    dp.callback_query.register(category_futures_futures_callback, F.data == "category_futures_futures")
    dp.callback_query.register(show_spread_details_callback, F.data.startswith("show_spread_futures_futures_"))
    
    dp.callback_query.register(select_convergence_callback, F.data.startswith("select_conv_") | F.data.startswith("track_"))
    dp.callback_query.register(set_convergence_val_callback, F.data.startswith("set_conv_") & ~F.data.startswith("set_conv_manual_"))
    dp.callback_query.register(set_convergence_manual_callback, F.data.startswith("set_conv_manual_"))
    
    dp.callback_query.register(show_tracked_list_callback, F.data == "show_tracked_list")
    dp.callback_query.register(show_tracked_details_callback, F.data.startswith("show_tracked_details_"))
    dp.callback_query.register(untrack_spread_callback, F.data.startswith("untrack_spread_"))
    
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
