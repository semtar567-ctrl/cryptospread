import asyncio
import time
import json
import logging
import structlog
import aiohttp.resolver
from collections import defaultdict
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiogram.fsm.context import FSMContext
from telegram_bot.utils.states import SettingsStates

# Фикс DNS для Windows: принудительно используем системный резолвер
aiohttp.resolver.DefaultResolver = aiohttp.resolver.ThreadedResolver

from aiogram import Bot, Dispatcher, types, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from telegram_bot.config import settings
from telegram_bot.database.engine import init_db, AsyncSessionLocal
from telegram_bot.database.models import User, UserSetting, UserExchange, TrackedSpread
from telegram_bot.utils.keyboards import (
    get_reply_main_menu, get_trade_types_keyboard, get_futures_settings_keyboard, 
    get_exchanges_keyboard, get_spreads_types_keyboard, format_signal_card_msg_and_kb
)
from telegram_bot.utils.redis_sync import sync_user_settings_to_redis, sync_all_users_to_redis
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
            reply_markup=get_spreads_types_keyboard(tracked_count)
        )

async def show_categories_callback(callback: types.CallbackQuery):
    async with AsyncSessionLocal() as session:
        stmt = select(TrackedSpread).where(TrackedSpread.user_id == callback.from_user.id, TrackedSpread.is_active == True)
        result = await session.execute(stmt)
        tracked_count = len(result.scalars().all())
        
        await callback.message.edit_text(
            "⚠️ **СВЯЗКИ**\n\nЗдесь можно посмотреть актуальные связки по выбранному типу сигналов.",
            parse_mode="Markdown",
            reply_markup=get_spreads_types_keyboard(tracked_count)
        )

async def category_futures_futures_callback(callback: types.CallbackQuery):
    from telegram_bot.utils.redis_sync import get_redis_client
    redis = get_redis_client()
    try:
        keys = []
        async for key in redis.scan_iter("current_spread:futures_futures:*"):
            keys.append(key)
        by_symbol = defaultdict(list)
        if keys:
            values = await redis.mget(keys)
            for data in values:
                if data:
                    try:
                        s = json.loads(data)
                        sym = s.get("symbol")
                        if sym:
                            by_symbol[sym].append(s)
                    except Exception:
                        pass

        symbol_best_pairs = []
        for symbol, pairs in by_symbol.items():
            best_pair = max(pairs, key=lambda s: float(s.get("vwap_net_spread") or 0.0))
            symbol_best_pairs.append((symbol, best_pair))

        symbol_best_pairs.sort(key=lambda item: float(item[1].get("vwap_net_spread") or 0.0), reverse=True)

        text = "🟢 <b>СВЯЗКИ: ФЬЮЧЕРС/ФЬЮЧЕРС МЕЖБИРЖ</b>\n\nВыберите связку для просмотра подробной информации:\n\n"
        buttons = []

        if not symbol_best_pairs:
            text += "<i>(нет активных связок в данный момент)</i>"
        else:
            for i, (symbol, s) in enumerate(symbol_best_pairs, 1):
                buy_ex = s.get('buy_ex', '').upper()
                sell_ex = s.get('sell_ex', '').upper()
                spread_val = float(s.get('vwap_net_spread') or 0.0)
                line_str = f"{i}. {symbol} ({buy_ex} -> {sell_ex}) | <b>{spread_val:.2f}%</b>"
                text += line_str + "\n"

                btn_text = f"{symbol} ({buy_ex}->{sell_ex}) | {spread_val:.2f}%"
                buttons.append([types.InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"show_symbol_card_{symbol}"
                )])

        buttons.append([
            types.InlineKeyboardButton(text="🔄 Обновить", callback_data="category_futures_futures"),
            types.InlineKeyboardButton(text="⬅️ Назад", callback_data="show_categories")
        ])

        kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.error("Error in category_futures_futures_callback", error=str(e))
        await callback.answer("Ошибка при загрузке связок", show_alert=True)


async def show_symbol_card_callback(callback: types.CallbackQuery):
    if callback.data.startswith("show_symbol_card_"):
        symbol = callback.data.replace("show_symbol_card_", "")
    elif callback.data.startswith("show_spread_futures_futures_"):
        parts = callback.data.split("_")
        if len(parts) >= 5:
            symbol = parts[4]
        else:
            symbol = ""
    else:
        symbol = ""

    if not symbol:
        await callback.answer("Ошибка формата данных", show_alert=True)
        return

    from telegram_bot.utils.redis_sync import get_redis_client
    redis = get_redis_client()
    try:
        keys = []
        async for key in redis.scan_iter(f"current_spread:futures_futures:{symbol}:*"):
            keys.append(key)
        pairs = []
        if keys:
            data_list = await redis.mget(keys)
            for data_raw in data_list:
                if data_raw:
                    try:
                        pairs.append(json.loads(data_raw))
                    except Exception:
                        pass

        if not pairs:
            await callback.answer("⚠️ Эта связка больше не активна (спред ушел).", show_alert=True)
            await category_futures_futures_callback(callback)
            return

        pairs.sort(key=lambda p: float(p.get("vwap_net_spread") or 0.0), reverse=True)

        user_id = callback.from_user.id
        symbol_signal_key = f"symbol_signal:{user_id}:{symbol}"
        data_to_store = {
            "symbol": symbol,
            "pairs": pairs,
            "updated_at": time.time()
        }
        await redis.set(symbol_signal_key, json.dumps(data_to_store), ex=300)

        text, kb = format_signal_card_msg_and_kb(symbol, pairs, idx=0)
        try:
            await callback.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True
            )
            await callback.answer()
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                await callback.answer()
            else:
                raise e
    except Exception as e:
        logger.error("Error in show_symbol_card_callback", error=str(e))
        await callback.answer("Ошибка при отображении карточки связки", show_alert=True)

show_spread_details_callback = show_symbol_card_callback

def _get_ex_link_main(exchange, symbol):
    return {
        "binance": f"https://www.binance.com/en/futures/{symbol}",
        "bybit": f"https://www.bybit.com/trade/usdt/{symbol}",
        "okx": f"https://www.okx.com/trade-swap/{symbol.lower()}-usdt",
        "mexc": f"https://contract.mexc.com/exchange/{symbol}_USDT",
        "bitget": f"https://www.bitget.com/futures/usdt/{symbol}",
        "gateio": f"https://www.gate.io/futures/USDT/{symbol}_USDT",
        "bingx": f"https://bingx.com/en/futures/forward/{symbol}USDT",
        "kucoinfutures": f"https://www.kucoin.com/trade/futures/{symbol}M",
        "hyperliquid": f"https://app.hyperliquid.xyz/trade/{symbol}",
        "asterdex": f"https://asterdex.com/en/futures/{symbol}",
        "bitmart": f"https://www.bitmart.com/trade/en-US?symbol={symbol}&type=contract"
    }.get(exchange.lower(), "#")

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
        
        await sync_user_settings_to_redis(callback.from_user.id, session)
        
        await callback.answer(f"🚀 Начал отслеживание {symbol}!\nУведомление придет при сходе до {val}%.", show_alert=True)
        
        callback = callback.model_copy(update={"data": f"show_spread_futures_futures_{symbol}_{buy_ex}_{sell_ex}"})
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
        
        from telegram_bot.utils.redis_sync import get_redis_client
        redis = get_redis_client()
        current_spread_val = "н/д"
        
        key = f"current_spread:futures_futures:{record.symbol}:{record.buy_exchange}:{record.sell_exchange}"
        data_raw = await redis.get(key)
        if data_raw:
            s = json.loads(data_raw)
            current_spread_val = f"{s['vwap_net_spread']:.2f}%"
            
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
            for ex_id in ["binance", "bybit", "okx", "mexc", "bitget", "gateio", "bingx", "kucoinfutures", "hyperliquid", "asterdex", "htx", "dydx", "lbank", "xt", "coinexfutures"]:
                session.add(UserExchange(user_id=user_id, exchange_id=ex_id))
            await session.commit()
            await message.answer(f"👋 Добро пожаловать, {username or 'друг'}!", reply_markup=get_reply_main_menu())
        else:
            # Проверяем и добавляем новые биржи (если они были добавлены в систему позже)
            stmt_ex = select(UserExchange).where(UserExchange.user_id == user_id)
            result_ex = await session.execute(stmt_ex)
            existing_ex = {ex.exchange_id for ex in result_ex.scalars().all()}
            for ex_id in ["binance", "bybit", "okx", "mexc", "bitget", "gateio", "bingx", "kucoinfutures", "hyperliquid", "asterdex", "htx", "dydx", "lbank", "xt", "coinexfutures"]:
                if ex_id not in existing_ex:
                    session.add(UserExchange(user_id=user_id, exchange_id=ex_id))
            await session.commit()
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
        
        required_exchanges = ["binance", "bybit", "okx", "mexc", "bitget", "gateio", "bingx", "kucoinfutures", "hyperliquid", "asterdex", "htx", "dydx", "lbank", "xt", "coinexfutures"]
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
    await callback.message.answer("Введите минимальный суточный объем в USD (например, 1000000):")
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

async def sympage_callback(callback: types.CallbackQuery):
    """Обработка переключения страниц (бирж) в карточке сигнала: sympage:{symbol}:{idx}."""
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("Ошибка формата данных", show_alert=True)
        return

    symbol = parts[1]
    try:
        idx = int(parts[2])
    except ValueError:
        await callback.answer("Ошибка индекса", show_alert=True)
        return

    user_id = callback.from_user.id

    from telegram_bot.utils.redis_sync import get_redis_client
    redis = get_redis_client()
    try:
        key = f"symbol_signal:{user_id}:{symbol}"
        raw_data = await redis.get(key)
        if not raw_data:
            await callback.answer("⚠️ Данные сигнала устарели или истекли.", show_alert=True)
            return

        data = json.loads(raw_data)
        pairs = data.get("pairs", [])
        if not pairs:
            await callback.answer("⚠️ Активные связки не найдены.", show_alert=True)
            return

        text, kb = format_signal_card_msg_and_kb(symbol, pairs, idx)
        try:
            await callback.message.edit_text(
                text, 
                parse_mode="HTML", 
                reply_markup=kb, 
                disable_web_page_preview=True
            )
            await callback.answer()
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                await callback.answer("Уже на этой странице 👌")
            else:
                raise e
    except Exception as e:
        logger.error("Error in sympage_callback", error=str(e))
        await callback.answer("Ошибка при переключении.", show_alert=True)


async def symref_callback(callback: types.CallbackQuery):
    """Обновление данных карточки сигнала по запросу пользователя: symref:{symbol}:{idx}."""
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("Ошибка формата данных", show_alert=True)
        return

    symbol = parts[1]
    try:
        idx = int(parts[2])
    except ValueError:
        await callback.answer("Ошибка индекса", show_alert=True)
        return

    user_id = callback.from_user.id

    from telegram_bot.utils.redis_sync import get_redis_client
    redis = get_redis_client()
    try:
        spread_keys = []
        async for skey in redis.scan_iter(f"current_spread:futures_futures:{symbol}:*"):
            spread_keys.append(skey)
        fresh_pairs = []
        if spread_keys:
            sdata_list = await redis.mget(spread_keys)
            for sdata in sdata_list:
                if sdata:
                    try:
                        fresh_pairs.append(json.loads(sdata))
                    except: pass

        key = f"symbol_signal:{user_id}:{symbol}"
        raw_data = await redis.get(key)

        if fresh_pairs:
            fresh_pairs.sort(key=lambda p: p.get("vwap_net_spread", 0.0), reverse=True)
            pairs = fresh_pairs
            data_to_store = {
                "symbol": symbol,
                "pairs": pairs,
                "updated_at": time.time()
            }
            await redis.set(key, json.dumps(data_to_store), ex=300)
        elif raw_data:
            data = json.loads(raw_data)
            pairs = data.get("pairs", [])
        else:
            await callback.answer("⚠️ Данные сигнала истекли.", show_alert=True)
            return

        if not pairs:
            await callback.answer("⚠️ Активные связки не найдены.", show_alert=True)
            return

        text, kb = format_signal_card_msg_and_kb(symbol, pairs, idx)
        try:
            await callback.message.edit_text(
                text, 
                parse_mode="HTML", 
                reply_markup=kb, 
                disable_web_page_preview=True
            )
            await callback.answer("🔄 Данные успешно обновлены!")
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                await callback.answer("Данные уже актуальны 👌")
            else:
                raise e
    except Exception as e:
        logger.error("Error in symref_callback", error=str(e))
        await callback.answer("Ошибка при обновлении данных.", show_alert=True)


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
    dp.callback_query.register(show_symbol_card_callback, F.data.startswith("show_symbol_card_") | F.data.startswith("show_spread_futures_futures_"))
    
    dp.callback_query.register(select_convergence_callback, F.data.startswith("select_conv_") | F.data.startswith("track_"))
    dp.callback_query.register(set_convergence_val_callback, F.data.startswith("set_conv_") & ~F.data.startswith("set_conv_manual_"))
    dp.callback_query.register(set_convergence_manual_callback, F.data.startswith("set_conv_manual_"))
    
    dp.callback_query.register(show_tracked_list_callback, F.data == "show_tracked_list")
    dp.callback_query.register(show_tracked_details_callback, F.data.startswith("show_tracked_details_"))
    dp.callback_query.register(untrack_spread_callback, F.data.startswith("untrack_spread_"))
    
    dp.callback_query.register(close_menu_callback, F.data == "close_menu")
    
    # Пагинатор и обновление карточки сигнала
    dp.callback_query.register(sympage_callback, F.data.startswith("sympage:"))
    dp.callback_query.register(symref_callback, F.data.startswith("symref:"))
    
    # Заглушки для типов торговли и информационных кнопок
    dp.callback_query.register(placeholder_callback, F.data.startswith("trade_") | F.data.in_({"none", "placeholder", "hint"}))

    logger.info("Starting Telegram Bot...")
    try:
        await dp.start_polling(bot)
    finally:
        from telegram_bot.utils.redis_sync import close_redis_client
        await close_redis_client()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
