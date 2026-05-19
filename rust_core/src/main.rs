mod config;
mod models;
mod collectors;
mod engine;

use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::Arc;
use std::collections::HashMap;
use tracing::{info, error};
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Инициализация логгера
    tracing_subscriber::fmt::init();
    
    info!("Starting CryptoSpread Rust Core...");

    // Загрузка конфигурации
    let config = match AppConfig::new() {
        Ok(cfg) => {
            info!("Config loaded: {} symbols tracked", cfg.symbols.len());
            cfg
        },
        Err(e) => {
            error!("Failed to load config: {}", e);
            return Err(anyhow::anyhow!(e));
        }
    };

    // Инициализация SpreadEngine
    let engine = Arc::new(SpreadEngine::new(config.clone())?);

    // Канал для уведомления о смене белого списка монет
    let (whitelist_tx, _) = tokio::sync::broadcast::channel::<HashMap<String, Vec<String>>>(16);

    // Фоновая задача: мониторинг белого списка в Redis
    let redis_config = config.redis.clone();
    let wl_tx = whitelist_tx.clone();
    tokio::spawn(async move {
        let redis_url = format!("redis://{}:{}", redis_config.host, redis_config.port);
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let mut last_whitelist = String::new();

        info!("Whitelist monitor started");
        loop {
            if let Ok(json_str) = redis::AsyncCommands::get::<_, String>(&mut conn, "active_symbols_map").await {
                if json_str != last_whitelist {
                    if let Ok(map) = serde_json::from_str::<HashMap<String, Vec<String>>>(&json_str) {
                        info!("New whitelist detected: {} symbols", map.len());
                        let _ = wl_tx.send(map);
                        last_whitelist = json_str;
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    });

    // Запуск Binance Futures коллектора
    let binance_config = config.clone();
    let binance_engine = Arc::clone(&engine);
    let mut binance_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::binance_futures::BinanceFuturesCollector::new(binance_config, binance_engine);
        collector.run_dynamic(&mut binance_wl_rx).await;
    });

    // Запуск Bybit Futures коллектора
    let bybit_config = config.clone();
    let bybit_engine = Arc::clone(&engine);
    let mut bybit_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::bybit_futures::BybitFuturesCollector::new(bybit_config, bybit_engine);
        collector.run_dynamic(&mut bybit_wl_rx).await;
    });

    // Запуск OKX Futures коллектора
    let okx_config = config.clone();
    let okx_engine = Arc::clone(&engine);
    let mut okx_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::okx_futures::OkxFuturesCollector::new(okx_config, okx_engine);
        collector.run_dynamic(&mut okx_wl_rx).await;
    });

    info!("Rust Core initialized. Binance, Bybit, and OKX collectors started.");
    
    // Временная заглушка, чтобы main не завершался сразу
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}
