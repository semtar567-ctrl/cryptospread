use serde::Deserialize;
use std::collections::HashMap;
use config::{Config, ConfigError, File};

#[derive(Debug, Deserialize, Clone)]
pub struct ExchangeConfig {
    pub enabled: bool,
    pub taker_fee: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SignalConfig {
    pub min_spread_percent: f64,
    pub min_liquidity_usd: f64,
    pub alert_cooldown_seconds: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    pub stream_name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub project_name: String,
    pub symbols: Vec<String>,
    pub exchanges: HashMap<String, ExchangeConfig>,
    pub signals: SignalConfig,
    pub redis: RedisConfig,
}

impl AppConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(File::with_name("configs/settings.yaml").required(false))
            .add_source(File::with_name("../configs/settings.yaml").required(false))
            .build()?;
        s.try_deserialize()
    }
}
