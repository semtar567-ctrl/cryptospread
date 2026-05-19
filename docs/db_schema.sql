-- Схема базы данных для CryptoSpread (SQLite/PostgreSQL)

-- Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,          -- Telegram ID
    username TEXT,
    is_admin BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Настройки фильтров пользователя (отдельно для каждого типа торговли)
CREATE TABLE IF NOT EXISTS user_settings (
    user_id INTEGER REFERENCES users(user_id),
    trade_type TEXT NOT NULL,             -- 'futures_futures', 'spot_futures', 'funding'
    min_spread_percent REAL DEFAULT 0.1,  -- Минимальный спред в %
    min_volume_24h REAL DEFAULT 1000000,  -- Минимальный объем монеты за 24ч в USD
    working_volume_usd REAL DEFAULT 1000, -- Рабочий объем пользователя для расчета проскальзывания
    min_lifetime_seconds INTEGER DEFAULT 3,-- Время жизни связки в секундах
    is_enabled BOOLEAN DEFAULT TRUE,      -- Включен ли этот тип торговли вообще
    
    -- Переключатели фильтров (вкл/выкл каждый)
    filter_spread_enabled BOOLEAN DEFAULT TRUE,
    filter_volume_enabled BOOLEAN DEFAULT TRUE,
    filter_slippage_enabled BOOLEAN DEFAULT TRUE,
    filter_lifetime_enabled BOOLEAN DEFAULT TRUE,
    
    PRIMARY KEY (user_id, trade_type)
);

-- Настройки бирж для пользователя (какие биржи он хочет мониторить)
CREATE TABLE IF NOT EXISTS user_exchanges (
    user_id INTEGER REFERENCES users(user_id),
    exchange_id TEXT NOT NULL,            -- 'binance', 'bybit', 'okx'
    is_enabled BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (user_id, exchange_id)
);

-- Отслеживаемые связки (схождение)
CREATE TABLE IF NOT EXISTS tracked_spreads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(user_id),
    symbol TEXT NOT NULL,                 -- Например, 'BTCUSDT'
    buy_exchange TEXT NOT NULL,
    sell_exchange TEXT NOT NULL,
    entry_spread REAL,                    -- Спред в момент входа
    target_convergence REAL DEFAULT 0.05, -- Целевой процент схождения для уведомления
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE        -- Активно ли отслеживание сейчас
);
