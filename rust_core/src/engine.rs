use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as TokioMutex;
use tracing::{info, error};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use serde_json::json;
use std::time::{Instant, Duration};

pub struct SpreadEngine {
    config: AppConfig,
    // Хранилище последних стаканов: symbol -> exchange -> update
    state: Arc<StdMutex<HashMap<String, HashMap<String, OrderBookUpdate>>>>,
    redis_client: Client,
    // Кэш настроек из Redis
    cached_min_spread: StdMutex<(f64, Instant)>,
    // Общее соединение для записи сигналов (используем асинхронный мьютекс)
    redis_conn: TokioMutex<Option<MultiplexedConnection>>,
}

impl SpreadEngine {
    pub fn new(config: AppConfig) -> anyhow::Result<Self> {
        let redis_url = format!("redis://{}:{}", config.redis.host, config.redis.port);
        let redis_client = Client::open(redis_url)?;
        
        Ok(Self {
            config,
            state: Arc::new(StdMutex::new(HashMap::new())),
            redis_client,
            cached_min_spread: StdMutex::new((0.1, Instant::now() - Duration::from_secs(60))),
            redis_conn: TokioMutex::new(None),
        })
    }

    async fn get_connection(&self) -> anyhow::Result<MultiplexedConnection> {
        let mut conn_guard = self.redis_conn.lock().await;
        if let Some(conn) = &*conn_guard {
            return Ok(conn.clone());
        }
        
        let conn = self.redis_client.get_multiplexed_async_connection().await?;
        *conn_guard = Some(conn.clone());
        Ok(conn)
    }

    async fn get_min_spread(&self) -> f64 {
        {
            let cache = self.cached_min_spread.lock().unwrap();
            if cache.1.elapsed() < Duration::from_secs(5) {
                return cache.0;
            }
        }

        // Обновляем кэш
        match self.get_connection().await {
            Ok(mut conn) => {
                let val: Result<String, _> = conn.get("global_min_spread").await;
                let spread = match val {
                    Ok(s) => s.parse().unwrap_or(self.config.signals.min_spread_percent),
                    Err(_) => self.config.signals.min_spread_percent,
                };
                let mut cache = self.cached_min_spread.lock().unwrap();
                *cache = (spread, Instant::now());
                spread
            }
            Err(_) => {
                let cache = self.cached_min_spread.lock().unwrap();
                cache.0
            }
        }
    }

    pub async fn process_update(&self, update: OrderBookUpdate) -> anyhow::Result<()> {
        let symbol = update.symbol.clone();
        let exchange = update.exchange.clone();

        // 1. Обновляем состояние
        {
            let mut state = self.state.lock().unwrap();
            let symbol_entry = state.entry(symbol.clone()).or_insert_with(|| HashMap::new());
            symbol_entry.insert(exchange.clone(), update);
        }

        // 2. Ищем арбитражные возможности для этого символа
        self.check_arbitrage(&symbol).await?;

        Ok(())
    }

    async fn check_arbitrage(&self, symbol: &str) -> anyhow::Result<()> {
        let exchange_data: Vec<(String, OrderBookUpdate)> = {
            let state = self.state.lock().unwrap();
            let symbol_data = match state.get(symbol) {
                Some(data) => data,
                None => return Ok(()),
            };
            
            symbol_data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        if exchange_data.len() < 2 {
            return Ok(());
        }

        // Получаем порог один раз для всех сравнений этого символа
        let min_spread = self.get_min_spread().await;

        // Compare every pair of exchanges dynamically
        for i in 0..exchange_data.len() {
            for j in (i + 1)..exchange_data.len() {
                let (ex1_name, ex1_data) = &exchange_data[i];
                let (ex2_name, ex2_data) = &exchange_data[j];

                // Сценарий А: Ex1 -> Ex2
                if !ex1_data.asks.is_empty() && !ex2_data.bids.is_empty() {
                    self.calculate_and_report(symbol, ex1_name, ex2_name, ex1_data.asks[0].0, ex2_data.bids[0].0, min_spread).await?;
                }

                // Сценарий Б: Ex2 -> Ex1
                if !ex2_data.asks.is_empty() && !ex1_data.bids.is_empty() {
                    self.calculate_and_report(symbol, ex2_name, ex1_name, ex2_data.asks[0].0, ex1_data.bids[0].0, min_spread).await?;
                }
            }
        }

        Ok(())
    }

    async fn calculate_and_report(&self, symbol: &str, buy_ex: &str, sell_ex: &str, buy_price: f64, sell_price: f64, min_spread: f64) -> anyhow::Result<()> {
        let spread_raw = sell_price - buy_price;
        let spread_percent = (spread_raw / buy_price) * 100.0;

        // Учет комиссий бирж (базовая проверка для Rust)
        let buy_fee = self.config.exchanges.get(&buy_ex.to_lowercase()).map(|e| e.taker_fee).unwrap_or(0.0006);
        let sell_fee = self.config.exchanges.get(&sell_ex.to_lowercase()).map(|e| e.taker_fee).unwrap_or(0.0006);

        let net_spread_percent = spread_percent - (buy_fee + sell_fee) * 100.0;

        if net_spread_percent >= min_spread {
            info!("🚀 POTENTIAL SPREAD: {} {}->{} | Net: {:.3}%", 
                  symbol, buy_ex, sell_ex, net_spread_percent);
            
            let (buy_book, sell_book) = {
                let state = self.state.lock().unwrap();
                let symbol_data = state.get(symbol).unwrap();
                (symbol_data.get(&buy_ex.to_lowercase()).unwrap().clone(), 
                 symbol_data.get(&sell_ex.to_lowercase()).unwrap().clone())
            };

            let signal = json!({
                "symbol": symbol,
                "buy_on": buy_ex,
                "sell_on": sell_ex,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "net_spread": net_spread_percent,
                "timestamp": chrono::Utc::now().timestamp_millis(),
                "buy_book": { "bids": buy_book.bids, "asks": buy_book.asks },
                "sell_book": { "bids": sell_book.bids, "asks": sell_book.asks }
            });

            match self.get_connection().await {
                Ok(mut conn) => {
                    let _: () = conn.xadd(
                        &self.config.redis.stream_name,
                        "*",
                        &[("data", signal.to_string())]
                    ).await?;
                }
                Err(e) => error!("Failed to write signal to Redis: {}", e),
            }
        }

        Ok(())
    }
}
