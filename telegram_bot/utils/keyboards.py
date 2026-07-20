from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

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
        [InlineKeyboardButton(text=get_on_off(settings.filter_slippage_enabled), callback_data="toggle_filter_slippage"),
         InlineKeyboardButton(text=f"🌐 >${settings.working_volume_usd:g}", callback_data="edit_work_vol")],
        
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
