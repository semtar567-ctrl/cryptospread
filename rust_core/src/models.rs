use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderBookUpdate {
    pub exchange: String,
    pub symbol: String,
    pub bids: Vec<(f64, f64)>, // (price, amount)
    pub asks: Vec<(f64, f64)>, // (price, amount)
    pub timestamp: u64,
}
