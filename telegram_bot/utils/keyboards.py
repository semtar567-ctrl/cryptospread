from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo

def get_reply_main_menu(is_on: bool = False):
    """Нижняя панель кнопок под полем ввода."""
    on_off_text = "🟢 On" if is_on else "🔴 Off"
    keyboard = [
        [KeyboardButton(text="🔔 Связки"), KeyboardButton(text="⚙️ Меню")],
        [KeyboardButton(text=on_off_text), KeyboardButton(text="👤 Профиль")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_trade_types_keyboard(tracked_count: int = 0):
    """Главное инлайн-меню выбора типов торговли (как на скрине)."""
    buttons = [
        [InlineKeyboardButton(text="🟢🔴 Спот/Фьючерс Внутрибирж", callback_data="trade_spot_futures_intra")],
        [InlineKeyboardButton(text="🟢⚪ Спот/Фьючерс Межбирж", callback_data="trade_spot_futures_inter")],
        [InlineKeyboardButton(text="🟦🟧 Фьючерс/Фьючерс Межбирж", callback_data="settings_futures_futures")],
        [InlineKeyboardButton(text="🔹🔺 Арбитраж Фандинга", callback_data="trade_funding")],
        [InlineKeyboardButton(text="🏛 Биржи:", callback_data="settings_exchanges")],
        [InlineKeyboardButton(text=f"📍 Отслеживаемые: {tracked_count}", callback_data="show_tracked_list")],
        [InlineKeyboardButton(text="❓ Подсказка", callback_data="hint"), 
         InlineKeyboardButton(text="❌ Закрыть", callback_data="close_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_spreads_types_keyboard(tracked_count: int = 0):
    """Главное инлайн-меню выбора типов торговли для просмотра связок."""
    buttons = [
        [InlineKeyboardButton(text="🟢🔴 Спот/Фьючерс Внутрибирж", callback_data="trade_spot_futures_intra")],
        [InlineKeyboardButton(text="🟢⚪ Спот/Фьючерс Межбирж", callback_data="trade_spot_futures_inter")],
        [InlineKeyboardButton(text="🟦🟧 Фьючерс/Фьючерс Межбирж", callback_data="category_futures_futures")],
        [InlineKeyboardButton(text="🔹🔺 Арбитраж Фандинга", callback_data="trade_funding")],
        [InlineKeyboardButton(text="🏛 Биржи:", callback_data="settings_exchanges")],
        [InlineKeyboardButton(text=f"📍 Отслеживаемые: {tracked_count}", callback_data="show_tracked_list")],
        [InlineKeyboardButton(text="❓ Подсказка", callback_data="hint"), 
         InlineKeyboardButton(text="❌ Закрыть", callback_data="close_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_futures_settings_keyboard(settings):
    """Детальное меню настроек фильтров (как на втором скрине)."""
    def get_on_off(enabled):
        return "✅ ON" if enabled else "❌ OFF"

    buttons = [
        # Сигналы ON/OFF (общий рубильник для этого типа торговли)
        [InlineKeyboardButton(text=f"🟢 Сигналы {get_on_off(settings.is_enabled)}", callback_data="toggle_futures_enabled")],
        
        # Курсовой спред
        [InlineKeyboardButton(text="📊 Курсовой Спред:", callback_data="none")],
        [InlineKeyboardButton(text=f"🌐 >{settings.min_spread_percent}%", callback_data="edit_spread")],
        
        # Объем сделки (Рабочий объем)
        [InlineKeyboardButton(text="💰 Объем сделки (Ликвидность):", callback_data="none")],
        [InlineKeyboardButton(text=f"🌐 >${settings.working_volume_usd:g}", callback_data="edit_work_vol")],
        
        # Объем за 24ч
        [InlineKeyboardButton(text="📈 Объем за 24ч:", callback_data="none")],
        [InlineKeyboardButton(text=get_on_off(settings.filter_volume_enabled), callback_data="toggle_filter_volume"),
         InlineKeyboardButton(text=f"🌐 >${settings.min_volume_24h:g}", callback_data="edit_volume")],
        
        # Проскальзывание
        [InlineKeyboardButton(text="📉 Проскальзывание (VWAP):", callback_data="none")],
        [InlineKeyboardButton(text=get_on_off(settings.filter_slippage_enabled), callback_data="toggle_filter_slippage"),
         InlineKeyboardButton(text="🌐 <0.1% (Limit)", callback_data="placeholder")],
        
        # Время жизни
        [InlineKeyboardButton(text="🕒 Время жизни:", callback_data="none")],
        [InlineKeyboardButton(text=get_on_off(settings.filter_lifetime_enabled), callback_data="toggle_filter_lifetime"),
         InlineKeyboardButton(text=f"🌐 >{settings.min_lifetime_seconds}с", callback_data="edit_lifetime")],
        
        # Навигация
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"),
         InlineKeyboardButton(text="❌ Закрыть", callback_data="close_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_exchanges_keyboard(user_exchanges):
    """Меню выбора бирж."""
    buttons = []
    # Формируем кнопки для каждой биржи с галочкой, если включена
    for ex in user_exchanges:
        status = "✅" if ex.is_enabled else "❌"
        buttons.append([InlineKeyboardButton(text=f"{status} {ex.exchange_id.capitalize()}", callback_data=f"toggle_ex_{ex.exchange_id}")])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_futures_menu_keyboard():
    """Промежуточное меню управления фьючерсным арбитражем."""
    buttons = [
        [InlineKeyboardButton(text="🔔 Посмотреть связки", callback_data="category_futures_futures")],
        [InlineKeyboardButton(text="⚙️ Настройки фильтров", callback_data="settings_futures_futures")],
        [InlineKeyboardButton(text="🏛 Выбор бирж", callback_data="settings_exchanges")],
        [InlineKeyboardButton(text="🚫 Черный список", callback_data="settings_blacklist")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"),
         InlineKeyboardButton(text="❌ Закрыть", callback_data="close_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_blacklist_keyboard(blacklist_items):
    """Меню черного списка монет."""
    buttons = []
    for item in blacklist_items:
        buttons.append([
            InlineKeyboardButton(text=f"🚫 {item.symbol}", callback_data="none"),
            InlineKeyboardButton(text="❌ Удалить", callback_data=f"remove_bl_{item.id}")
        ])
    buttons.append([InlineKeyboardButton(text="➕ Добавить монету", callback_data="add_to_blacklist")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_ex_link(exchange: str, symbol: str) -> str:
    ex = exchange.lower()
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
        "bitmart": f"https://www.bitmart.com/trade/en-US?symbol={symbol}&type=contract",
        "htx": f"https://www.htx.com/trade/{symbol.lower()}_usdt",
        "dydx": f"https://dydx.trade/trade/{symbol.replace('USDT', '').replace('USD', '')}-USD",
        "lbank": f"https://www.lbank.com/futures/{symbol.lower()}/",
        "xt": f"https://www.xt.com/en/futures/trade/{symbol.lower()}",
        "coinexfutures": f"https://www.coinex.com/en/futures/market/{symbol}"
    }.get(ex, "#")

def format_time_to_funding(ts_ms) -> str:
    import time
    from datetime import datetime, timedelta, timezone
    if not ts_ms:
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
        dt_msk = datetime.fromtimestamp(ts_sec, timezone(timedelta(hours=3)))
        time_str = dt_msk.strftime("%H:%M")
        
        if diff < 0: 
            return f"{time_str} (сейчас)"
        
        hours = int(diff // 3600)
        minutes = int((diff % 3600) // 60)
        return f"{time_str} (через {hours}ч {minutes}м)"
    except: 
        return "н/д"

def format_vol_usd(vol: float) -> str:
    try:
        val = float(vol or 0.0)
    except (ValueError, TypeError):
        val = 0.0

    if val >= 1_000_000:
        return f"${val / 1e6:.2f}M"
    elif val >= 1_000:
        return f"${val / 1e3:.1f}K"
    else:
        return f"${val:.0f}"

def format_signal_card_msg_and_kb(symbol: str, pairs: list, idx: int = 0):
    """
    Формирует сообщение-карточку и клавиатуру в стиле @arby_futures_bot.
    Использует HTML разметку для безопасной обработки ссылок.
    """
    total_pages = len(pairs)
    if total_pages == 0:
        return "⚠️ Нет активных сигналов.", InlineKeyboardMarkup(inline_keyboard=[])

    if idx < 0 or idx >= total_pages:
        idx = 0

    s = pairs[idx]
    buy_ex = s['buy_ex']
    sell_ex = s['sell_ex']
    buy_ex_upper = buy_ex.upper()
    sell_ex_upper = sell_ex.upper()

    import os
    from telegram_bot.config import settings
    chart_base_url = (getattr(settings, "spread_chart_url", "") or os.getenv("SPREAD_CHART_URL", "")).rstrip("/")
    if not chart_base_url:
        chart_base_url = "https://happening-rica-plains-hose.trycloudflare.com"
    chart_link = f"{chart_base_url}/?buy={buy_ex}&sell={sell_ex}&symbol={symbol}"

    warn_b = " ⚠️ Низкая ликвидность" if not s.get("buy_full", True) else ""
    warn_s = " ⚠️ Низкая ликвидность" if not s.get("sell_full", True) else ""

    text = (
        f"🟢🔴 <b>${symbol}</b> (F/F)\n"
        f"↑ {buy_ex_upper} F. / ↓ {sell_ex_upper} F.\n\n"
        f"🏛 <b>Курсовой Спред:</b> {s['net_spread']:.2f}%\n"
        f"💎 <b>Фандинг Спред:</b> {s['funding_spread']:.3f}%\n"
        f"💸 <b>Комиссии:</b> {s['total_fee']:.3f}%\n"
        f"📊 <b>Общий спред (VWAP):</b> <b>{s['vwap_net_spread']:.2f}%</b>\n"
        f"💰 <b>Объем:</b> ${s['vol_usd']:g}\n\n"
        f"🟩 <b>ЛОНГ НА ФЬЮЧАХ:</b>\n"
        f'<a href="{get_ex_link(buy_ex, symbol)}">{buy_ex_upper} Futures</a>\n'
        f"Цена: {s['buy_price']:.5f}\n"
        f"Фандинг: {s['buy_funding']:.3f}% | {format_time_to_funding(s['buy_next_f'])}\n"
        f"Объем 24ч: {format_vol_usd(s.get('buy_vol_24h', 0))}\n"
        f"Проскальзывание: {s['buy_slip']:.3f}%{warn_b}\n\n"
        f"🟥 <b>ШОРТ НА ФЬЮЧАХ:</b>\n"
        f'<a href="{get_ex_link(sell_ex, symbol)}">{sell_ex_upper} Futures</a>\n'
        f"Цена: {s['sell_price']:.5f}\n"
        f"Фандинг: {s['sell_funding']:.3f}% | {format_time_to_funding(s['sell_next_f'])}\n"
        f"Объем 24ч: {format_vol_usd(s.get('sell_vol_24h', 0))}\n"
        f"Проскальзывание: {s['sell_slip']:.3f}%{warn_s}\n\n"
        f"==========="
    )

    other_pairs = [p for i, p in enumerate(pairs) if i != idx]
    if other_pairs:
        text += f"\n🏛 <b>Еще {len(other_pairs)} на других биржах::</b>\n"
        for op in other_pairs:
            text += f"⚖️ {op['buy_ex'].upper()}/{op['sell_ex'].upper()}: P {op['vwap_net_spread']:.2f}%, F {op['funding_spread']:.2f}%\n"

    # Ряд 0: Кнопка Telegram WebApp (только если HTTPS, иначе обычный URL)
    if chart_link.startswith("https://"):
        row_webapp = [
            InlineKeyboardButton(text="📈 Открыть график спреда 1.0000 🔗", web_app=WebAppInfo(url=chart_link))
        ]
    else:
        row_webapp = [
            InlineKeyboardButton(text="📈 Открыть график спреда 1.0000 🔗", url=chart_link)
        ]

    # Ряд 1: Отслеживать + Обновить
    row1 = [
        InlineKeyboardButton(text="• Отслеживать", callback_data=f"track_{symbol}_{buy_ex}_{sell_ex}"),
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"symref:{symbol}:{idx}")
    ]

    # Ряд 2: Пагинатор < 1/N >
    prev_idx = (idx - 1) % total_pages
    next_idx = (idx + 1) % total_pages
    row2 = [
        InlineKeyboardButton(text="<", callback_data=f"sympage:{symbol}:{prev_idx}"),
        InlineKeyboardButton(text=f"{idx + 1}/{total_pages}", callback_data="none"),
        InlineKeyboardButton(text=">", callback_data=f"sympage:{symbol}:{next_idx}")
    ]

    keyboard = InlineKeyboardMarkup(inline_keyboard=[row_webapp, row1, row2])
    return text, keyboard
