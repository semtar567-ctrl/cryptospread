mod config;
mod models;
mod collectors;
mod engine;
mod user_manager;

use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::Arc;
use std::collections::HashMap;
use tracing::{info, error};
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Инициализация логгера с фильтрацией: ошибки и предупреждения для всех, инфо для нашего ядра
    tracing_subscriber::fmt()
        .with_env_filter("error,rust_core=info")
        .init();
    
    info!("Starting CryptoSpread Rust Core...");

    // Загрузка конфигурации
    let config = match AppConfig::new() {
        Ok(cfg) => {
            info!("Config loaded");
            cfg
        },
        Err(e) => {
            error!("Failed to load config: {}", e);
            return Err(anyhow::anyhow!(e));
        }
    };

    // Инициализация SpreadEngine
    let engine = Arc::new(SpreadEngine::new(config.clone()).await?);

    // Запуск слушателя Redis Pub/Sub для мгновенного обновления настроек
    let pubsub_engine = Arc::clone(&engine);
    tokio::spawn(async move {
        if let Err(e) = pubsub_engine.start_pubsub_listener().await {
            error!("Redis Pub/Sub listener error: {}", e);
        }
    });

    // Канал для уведомления о смене белого списка монет
    // Используем Value для поддержки гибкого формата от Python
    let (whitelist_tx, _) = tokio::sync::broadcast::channel::<HashMap<String, HashMap<String, serde_json::Value>>>(16);

    // Фоновая задача: мониторинг белого списка в Redis
    let redis_config = config.redis.clone();
    let wl_tx = whitelist_tx.clone();
    tokio::spawn(async move {
        let redis_url = format!("redis://{}:{}", redis_config.host, redis_config.port);
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();

        info!("Whitelist monitor started");
        loop {
            // Читаем список монет из Redis
            if let Ok(json_str) = redis::AsyncCommands::get::<_, String>(&mut conn, "active_symbols_map").await {
                if let Ok(map) = serde_json::from_str::<HashMap<String, HashMap<String, serde_json::Value>>>(&json_str) {
                    // Рассылаем вайтлист КАЖДЫЙ ТИК (раз в 10 сек), чтобы опоздавшие коллекторы его увидели
                    let _ = wl_tx.send(map);
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

    // Запуск MEXC Futures коллектора
    let mexc_config = config.clone();
    let mexc_engine = Arc::clone(&engine);
    let mut mexc_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::mexc_futures::MexcFuturesCollector::new(mexc_config, mexc_engine);
        collector.run_dynamic(&mut mexc_wl_rx).await;
    });

    // Запуск Bitget Futures коллектора
    let bitget_config = config.clone();
    let bitget_engine = Arc::clone(&engine);
    let mut bitget_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::bitget_futures::BitgetFuturesCollector::new(bitget_config, bitget_engine);
        collector.run_dynamic(&mut bitget_wl_rx).await;
    });

    // Запуск Gate.io Futures коллектора
    let gateio_config = config.clone();
    let gateio_engine = Arc::clone(&engine);
    let mut gateio_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::gateio_futures::GateioFuturesCollector::new(gateio_config, gateio_engine);
        collector.run_dynamic(&mut gateio_wl_rx).await;
    });

    // Запуск BingX Futures коллектора
    let bingx_config = config.clone();
    let bingx_engine = Arc::clone(&engine);
    let mut bingx_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::bingx_futures::BingxFuturesCollector::new(bingx_config, bingx_engine);
        collector.run_dynamic(&mut bingx_wl_rx).await;
    });

    // Запуск KuCoin Futures коллектора
    let kucoin_config = config.clone();
    let kucoin_engine = Arc::clone(&engine);
    let mut kucoin_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::kucoin_futures::KucoinFuturesCollector::new(kucoin_config, kucoin_engine);
        collector.run_dynamic(&mut kucoin_wl_rx).await;
    });

    // Запуск Hyperliquid Futures коллектора
    let hyperliquid_config = config.clone();
    let hyperliquid_engine = Arc::clone(&engine);
    let mut hyperliquid_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::hyperliquid_futures::HyperliquidFuturesCollector::new(hyperliquid_config, hyperliquid_engine);
        collector.run_dynamic(&mut hyperliquid_wl_rx).await;
    });

    // Запуск Aster DEX Futures коллектора
    let asterdex_config = config.clone();
    let asterdex_engine = Arc::clone(&engine);
    let mut asterdex_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::asterdex_futures::AsterdexFuturesCollector::new(asterdex_config, asterdex_engine);
        collector.run_dynamic(&mut asterdex_wl_rx).await;
    });

    // Запуск HTX Futures коллектора
    let htx_config = config.clone();
    let htx_engine = Arc::clone(&engine);
    let mut htx_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::htx_futures::HtxFuturesCollector::new(htx_config, htx_engine);
        collector.run_dynamic(&mut htx_wl_rx).await;
    });

    // Запуск dYdX v4 Futures коллектора
    let dydx_config = config.clone();
    let dydx_engine = Arc::clone(&engine);
    let mut dydx_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::dydx_futures::DydxFuturesCollector::new(dydx_config, dydx_engine);
        collector.run_dynamic(&mut dydx_wl_rx).await;
    });

    // Запуск LBank Futures коллектора
    let lbank_config = config.clone();
    let lbank_engine = Arc::clone(&engine);
    let mut lbank_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::lbank_futures::LbankFuturesCollector::new(lbank_config, lbank_engine);
        collector.run_dynamic(&mut lbank_wl_rx).await;
    });

    // Запуск XT Futures коллектора
    let xt_config = config.clone();
    let xt_engine = Arc::clone(&engine);
    let mut xt_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::xt_futures::XtFuturesCollector::new(xt_config, xt_engine);
        collector.run_dynamic(&mut xt_wl_rx).await;
    });

    // Запуск CoinEx Futures коллектора
    let coinex_config = config.clone();
    let coinex_engine = Arc::clone(&engine);
    let mut coinex_wl_rx = whitelist_tx.subscribe();
    tokio::spawn(async move {
        let mut collector = collectors::coinex_futures::CoinexFuturesCollector::new(coinex_config, coinex_engine);
        collector.run_dynamic(&mut coinex_wl_rx).await;
    });

    // Запуск основного цикла аналитики (раз в секунду)
    let tick_engine = Arc::clone(&engine);
    tokio::spawn(async move {
        if let Err(e) = tick_engine.start_tick_loop().await {
            error!("Tick Engine error: {}", e);
        }
    });

    info!("Rust Core initialized. 15 exchanges collectors started (Binance, Bybit, OKX, MEXC, Bitget, Gate.io, BingX, KuCoin, Hyperliquid, Aster DEX, HTX, dYdX, LBank, XT, CoinEx).");
    
    // Временная заглушка, чтобы main не завершался сразу
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}
