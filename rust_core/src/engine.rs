use crate::models::{OrderBookUpdate, OrderBookState, UserConfig};
use crate::config::AppConfig;
use crate::user_manager::UserManager;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as TokioMutex;
use tracing::{info, error, debug, warn};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use futures_util::StreamExt;
use serde_json::json;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;

pub struct SpreadEngine {
    config: AppConfig,
    // symbol -> (exchange -> state)
    state: DashMap<String, DashMap<String, OrderBookState>>,
    redis_client: Client,
    user_manager: UserManager,
    // Кэш настроек пользователей
    cached_users: StdMutex<(Vec<UserConfig>, Instant)>,
    // Флаг принудительного обновления кэша
    needs_refresh: std::sync::atomic::AtomicBool,
    // Общее соединение для записи сигналов (используем асинхронный мьютекс)
    redis_conn: TokioMutex<Option<MultiplexedConnection>>,
    
    // МЕХАНИЗМ УМНОГО ЛОГА
    // (symbol, buy, sell) -> last_log_time
    log_cooldown: StdMutex<HashMap<(String, String, String), Instant>>,
    
    // МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ
    msg_count: AtomicU64,
    start_time: Instant,
    last_metrics_report: StdMutex<Instant>,
}

impl SpreadEngine {
    pub async fn new(config: AppConfig) -> anyhow::Result<Self> {
        let redis_url = format!("redis://{}:{}", config.redis.host, config.redis.port);
        let redis_client = Client::open(redis_url.clone())?;
        let user_manager = UserManager::new(&config.database.url).await?;
        
        Ok(Self {
            config,
            state: DashMap::new(),
            redis_client,
            user_manager,
            cached_users: StdMutex::new((Vec::new(), Instant::now() - Duration::from_secs(600))),
            needs_refresh: std::sync::atomic::AtomicBool::new(false),
            redis_conn: TokioMutex::new(None),
            log_cooldown: StdMutex::new(HashMap::new()),
            msg_count: AtomicU64::new(0),
            start_time: Instant::now(),
            last_metrics_report: StdMutex::new(Instant::now()),
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

    async fn get_active_user_configs(&self) -> Vec<UserConfig> {
        let force_refresh = self.needs_refresh.swap(false, Ordering::Relaxed);
        
        {
            let cache = self.cached_users.lock().unwrap();
            if !force_refresh && cache.1.elapsed() < Duration::from_secs(60) {
                return cache.0.clone();
            }
        }

        if force_refresh {
            info!("🔄 User settings update received via Pub/Sub. Refreshing cache...");
        }

        match self.user_manager.get_all_active_users().await {
            Ok(users) => {
                info!("👤 Loaded {} active users from database", users.len());
                let mut cache = self.cached_users.lock().unwrap();
                *cache = (users.clone(), Instant::now());
                users
            }
            Err(e) => {
                error!("Failed to fetch user configs from PostgreSQL: {}", e);
                let cache = self.cached_users.lock().unwrap();
                cache.0.clone()
            }
        }
    }

    pub async fn process_update(&self, update: OrderBookUpdate) -> anyhow::Result<()> {
        self.msg_count.fetch_add(1, Ordering::Relaxed);
        
        {
            let mut last_report = self.last_metrics_report.lock().unwrap();
            if last_report.elapsed() >= Duration::from_secs(60) {
                let count = self.msg_count.swap(0, Ordering::Relaxed);
                let elapsed = last_report.elapsed().as_secs_f64();
                let tps = count as f64 / elapsed;
                let unique_symbols = self.state.len();
                
                info!("📊 PERFORMANCE: {:.0} msgs/sec | Unique symbols: {} | Total uptime: {} min", 
                    tps, unique_symbols, self.start_time.elapsed().as_secs() / 60);
                
                *last_report = Instant::now();
                let mut cooldown = self.log_cooldown.lock().unwrap();
                cooldown.retain(|_, v| v.elapsed() < Duration::from_secs(60));
            }
        }

        let symbol = update.symbol.clone();
        let exchange = update.exchange.clone();

        // Обновляем состояние (автоматически ставит is_dirty = true через From)
        let symbol_entry = self.state.entry(symbol).or_insert_with(DashMap::new);
        symbol_entry.insert(exchange, OrderBookState::from(update));

        Ok(())
    }

    pub async fn start_tick_loop(self: Arc<Self>) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(100)); // 100ms Tick
        info!("⏱️ Tick Engine started (100ms interval, with is_dirty optimization)");

        loop {
            interval.tick().await;
            
            // Получаем список всех символов
            let symbols: Vec<String> = self.state.iter().map(|item| item.key().clone()).collect();
            
            for symbol in symbols {
                // Проверяем, есть ли изменения по этому символу
                let is_dirty = if let Some(exchange_map) = self.state.get(&symbol) {
                    exchange_map.iter().any(|item| item.value().is_dirty)
                } else {
                    false
                };

                if is_dirty {
                    if let Err(e) = self.check_arbitrage(&symbol).await {
                        error!("Error checking arbitrage for {}: {}", symbol, e);
                    }

                    // После обработки сбрасываем флаги
                    if let Some(exchange_map) = self.state.get(&symbol) {
                        for mut item in exchange_map.iter_mut() {
                            item.value_mut().is_dirty = false;
                        }
                    }
                }
            }
        }
    }

    async fn check_arbitrage(&self, symbol: &str) -> anyhow::Result<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        
        let exchange_data: Vec<(String, OrderBookState)> = match self.state.get(symbol) {
            Some(symbol_data) => {
                symbol_data.iter()
                    .filter_map(|item| {
                        let state = item.value();
                        // ЗАЩИТА ОТ ПРОТУХШИХ ДАННЫХ: Игнорируем, если данные старше 3 секунд
                        if now_ms > state.local_timestamp && (now_ms - state.local_timestamp) > 3000 {
                            None
                        } else {
                            Some((item.key().clone(), state.clone()))
                        }
                    })
                    .collect()
            }
            None => return Ok(()),
        };

        if exchange_data.len() < 2 {
            return Ok(());
        }

        let active_users = self.get_active_user_configs().await;
        if active_users.is_empty() {
            return Ok(());
        }

        for i in 0..exchange_data.len() {
            for j in (i + 1)..exchange_data.len() {
                let (ex1_name, ex1_data) = &exchange_data[i];
                let (ex2_name, ex2_data) = &exchange_data[j];

                if !ex1_data.asks.is_empty() && !ex2_data.bids.is_empty() {
                    self.calculate_and_report(symbol, ex1_name, ex2_name, ex1_data.asks[0].0, ex2_data.bids[0].0, &active_users).await?;
                }

                if !ex2_data.asks.is_empty() && !ex1_data.bids.is_empty() {
                    self.calculate_and_report(symbol, ex2_name, ex1_name, ex2_data.asks[0].0, ex1_data.bids[0].0, &active_users).await?;
                }
            }
        }
        Ok(())
    }

    fn calculate_vwap(layers: &[(f64, f64)], target_usd: f64) -> (f64, f64) {
        let mut total_usd = 0.0;
        let mut total_amount = 0.0;

        for (price, amount) in layers {
            let layer_usd = price * amount;
            if total_usd + layer_usd >= target_usd {
                let remaining_usd = target_usd - total_usd;
                let partially_consumed_amount = remaining_usd / price;
                total_amount += partially_consumed_amount;
                total_usd = target_usd;
                break;
            } else {
                total_usd += layer_usd;
                total_amount += amount;
            }
        }

        if total_usd < target_usd || total_amount == 0.0 {
            (0.0, 0.0) 
        } else {
            (target_usd / total_amount, total_usd)
        }
    }

    async fn calculate_and_report(&self, symbol: &str, buy_ex: &str, sell_ex: &str, buy_price: f64, sell_price: f64, active_users: &[UserConfig]) -> anyhow::Result<()> {
        let spread_raw = sell_price - buy_price;
        let spread_percent = (spread_raw / buy_price) * 100.0;

        let buy_fee = self.config.exchanges.get(&buy_ex.to_lowercase()).map(|e| e.taker_fee).unwrap_or(0.0006);
        let sell_fee = self.config.exchanges.get(&sell_ex.to_lowercase()).map(|e| e.taker_fee).unwrap_or(0.0006);
        let net_spread_percent = spread_percent - (buy_fee + sell_fee) * 100.0;

        // РЕЖИМ ТОТАЛЬНОГО ЗРЕНИЯ: Показываем ВООБЩЕ ВСЕ расхождения цен для проверки
        info!("🔍 RAW CHECK: {} ({} -> {}) | Spread: {:.4}%", 
              symbol, buy_ex, sell_ex, net_spread_percent);

        let (buy_book, sell_book) = match self.state.get(symbol) {
            Some(symbol_data) => {
                let b = symbol_data.get(&buy_ex.to_lowercase()).map(|u| u.clone());
                let s = symbol_data.get(&sell_ex.to_lowercase()).map(|u| u.clone());
                match (b, s) {
                    (Some(b), Some(s)) => (b, s),
                    _ => return Ok(()),
                }
            }
            None => return Ok(()),
        };

        let mut target_users = Vec::new();
        let mut user_metrics = HashMap::new();

        for user in active_users {
            let settings = match &user.futures_settings {
                Some(s) => s,
                None => {
                    if user.user_id == 655686738 { debug!("User 655686738 skipped: No futures settings"); }
                    continue;
                }
            };

            if !user.enabled_exchanges.contains(&buy_ex.to_lowercase()) || 
               !user.enabled_exchanges.contains(&sell_ex.to_lowercase()) {
                if user.user_id == 655686738 {
                    debug!("User 655686738 skipped: Exchange not enabled. Buy: {}, Sell: {}, Enabled: {:?}", 
                        buy_ex, sell_ex, user.enabled_exchanges);
                }
                continue;
            }

            if settings.filter_spread_enabled == 1 && net_spread_percent < settings.min_spread_percent {
                if user.user_id == 655686738 {
                    debug!("User 655686738 skipped: Spread too low. Net: {:.4} < Min: {:.4}", 
                        net_spread_percent, settings.min_spread_percent);
                }
                continue;
            }

            let mut current_net_spread = net_spread_percent;
            let mut slippage = 0.0;

            let (vwap_buy, _) = Self::calculate_vwap(&buy_book.asks, settings.working_volume_usd);
            let (vwap_sell, _) = Self::calculate_vwap(&sell_book.bids, settings.working_volume_usd);

            if vwap_buy > 0.0 && vwap_sell > 0.0 {
                let eff_spread_percent = ((vwap_sell - vwap_buy) / vwap_buy) * 100.0;
                let eff_net_spread = eff_spread_percent - (buy_fee + sell_fee) * 100.0;
                slippage = net_spread_percent - eff_net_spread;
                
                // ВАЖНО: Решаем, по какой цене фильтровать
                if settings.filter_slippage_enabled == 1 {
                    current_net_spread = eff_net_spread;
                }
            } else if settings.filter_slippage_enabled == 1 {
                // Если объема нет и фильтр ВКЛ - скипаем
                continue;
            }

            // Основной фильтр по доходности (теперь либо по Best, либо по VWAP)
            if settings.filter_spread_enabled == 1 && current_net_spread < settings.min_spread_percent {
                continue;
            }

            target_users.push(user.user_id);
            user_metrics.insert(user.user_id.to_string(), json!({
                "vwap_buy": vwap_buy,
                "vwap_sell": vwap_sell,
                "slippage": slippage,
                "working_volume": settings.working_volume_usd
            }));
        }

        if !target_users.is_empty() {
            let log_key = (symbol.to_string(), buy_ex.to_string(), sell_ex.to_string());
            let should_log = {
                let mut cooldown = self.log_cooldown.lock().unwrap();
                let last_log = cooldown.get(&log_key);
                if last_log.is_none() || last_log.unwrap().elapsed() >= Duration::from_secs(5) {
                    cooldown.insert(log_key, Instant::now());
                    true
                } else {
                    false
                }
            };

            if should_log {
                info!("🚀 SPREAD: {} {}->{} | Raw Net: {:.3}% | Users: {:?}", 
                      symbol, buy_ex, sell_ex, net_spread_percent, target_users);
            }
            
            let signal = json!({
                "symbol": symbol,
                "buy_on": buy_ex,
                "sell_on": sell_ex,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "net_spread": net_spread_percent,
                "timestamp": chrono::Utc::now().timestamp_millis(),
                "buy_book": { "bids": buy_book.bids, "asks": buy_book.asks },
                "sell_book": { "bids": sell_book.bids, "asks": sell_book.asks },
                "target_users": target_users,
                "user_metrics": user_metrics
            });

            match self.get_connection().await {
                Ok(mut conn) => {
                    let res: anyhow::Result<()> = conn.xadd(
                        &self.config.redis.stream_name,
                        "*",
                        &[("data", signal.to_string())]
                    ).await.map_err(|e| anyhow::anyhow!(e));

                    if let Err(e) = res {
                        error!("Failed to write signal to Redis: {}. Invalidating connection.", e);
                        // СБРАСЫВАЕМ СОЕДИНЕНИЕ, чтобы в следующий раз создалось новое
                        let mut conn_guard = self.redis_conn.lock().await;
                        *conn_guard = None;
                    }
                }
                Err(e) => error!("Failed to get Redis connection: {}", e),
            }
        }
        Ok(())
    }

    pub async fn start_pubsub_listener(self: Arc<Self>) -> anyhow::Result<()> {
        let redis_url = format!("redis://{}:{}", self.config.redis.host, self.config.redis.port);
        let client = Client::open(redis_url)?;
        let mut pubsub = client.get_async_pubsub().await?;
        
        pubsub.subscribe("user_updates").await?;
        info!("📡 Redis Pub/Sub listener started on channel 'user_updates'");

        let mut stream = pubsub.on_message();
        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload()?;
            info!("🔔 Received settings update for user: {}", payload);
            self.needs_refresh.store(true, Ordering::Relaxed);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{OrderBookUpdate, OrderBookState};

    #[tokio::test]
    async fn test_market_state_update() {
        // Тестируем DashMap напрямую
        let state: DashMap<String, DashMap<String, OrderBookState>> = DashMap::new();

        let update = OrderBookUpdate {
            exchange: "binance".to_string(),
            symbol: "BTC/USDT".to_string(),
            bids: vec![(50000.0, 1.0)],
            asks: vec![(50001.0, 1.0)],
            timestamp: 12345,
            local_timestamp: 12345,
        };

        // Пишем данные (автоматическая конвертация в OrderBookState с is_dirty=true)
        state.entry(update.symbol.clone()).or_insert_with(DashMap::new)
            .insert(update.exchange.clone(), OrderBookState::from(update.clone()));

        // Читаем данные
        let symbol_data = state.get("BTC/USDT").unwrap();
        let binance_data = symbol_data.get("binance").unwrap();

        assert_eq!(binance_data.exchange, "binance");
        assert_eq!(binance_data.bids[0].0, 50000.0);
        assert_eq!(binance_data.is_dirty, true);
        info!("✅ Unit test: Market State (OrderBookState) works correctly.");
    }
}
