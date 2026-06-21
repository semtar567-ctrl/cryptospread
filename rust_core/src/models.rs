use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderBookUpdate {
    pub exchange: String,
    pub symbol: String,
    pub bids: Vec<(f64, f64)>, // (price, amount)
    pub asks: Vec<(f64, f64)>, // (price, amount)
    pub timestamp: u64,        // Exchange timestamp
    pub local_timestamp: u64,  // System arrival timestamp
    pub taker_fee: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct OrderBookState {
    pub exchange: String,
    pub symbol: String,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub timestamp: u64,
    pub local_timestamp: u64,
    pub taker_fee: f64,
    pub is_dirty: bool, // Флаг изменения для Tick Loop
}

impl From<OrderBookUpdate> for OrderBookState {
    fn from(update: OrderBookUpdate) -> Self {
        Self {
            exchange: update.exchange,
            symbol: update.symbol,
            bids: update.bids,
            asks: update.asks,
            timestamp: update.timestamp,
            local_timestamp: update.local_timestamp,
            taker_fee: update.taker_fee,
            is_dirty: true, // При создании из обновления всегда помечаем как "грязный"
        }
    }
}


#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UserTradeSettings {
    pub min_spread_percent: f64,
    pub min_volume_24h: f64,
    pub working_volume_usd: f64,
    pub min_lifetime_seconds: i32,
    pub is_enabled: i32, // Используем i32, так как Python отправляет 0/1
    pub filter_spread_enabled: i32,
    pub filter_volume_enabled: i32,
    pub filter_slippage_enabled: i32,
    pub filter_lifetime_enabled: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackedSpreadConfig {
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub target_convergence: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserConfig {
    pub user_id: i64,
    pub is_active: bool,
    pub futures_settings: Option<UserTradeSettings>,
    pub enabled_exchanges: Vec<String>,
    pub tracked_spreads: Vec<TrackedSpreadConfig>,
}
