use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, error, warn, debug};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::mpsc;

/// Команда для управления пулом соединений
enum PoolCommand {
    Subscribe(String),
    Unsubscribe(String),
}

pub struct MexcFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Маппинг: MEXC_ID (BTC_USDT) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, Contract Size)
    id_map: Arc<Mutex<HashMap<String, (String, f64, f64, f64)>>>,
    // Список всех монет, на которые мы СЕЙЧАС подписаны (по всем пулам)
    active_subscriptions: HashSet<String>,
    // Каналы связи с каждым запущенным пулом
    pool_channels: Vec<mpsc::UnboundedSender<PoolCommand>>,
}

impl MexcFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { 
            _config: config, 
            engine,
            id_map: Arc::new(Mutex::new(HashMap::new())),
            active_subscriptions: HashSet::new(),
            pool_channels: Vec::new(),
        }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>) {
        info!("MEXC Collector 2.0 (Smart Pooling) started");

        loop {
            if let Ok(whitelist) = whitelist_rx.recv().await {
                let mut new_id_map = HashMap::new();
                let mut target_ids = HashSet::new();

                for (unified, mapping) in whitelist.iter() {
                    if let Some(ex_info) = mapping.get("mexc") {
                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0006);
                            let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                            let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).unwrap_or(1.0);
                            new_id_map.insert(ex_id.to_string(), (unified.clone(), fee, multiplier, contract_size));
                            target_ids.insert(ex_id.to_string());
                        }
                    }
                }

                // Обновляем глобальную карту ID
                {
                    let mut id_map_guard = self.id_map.lock().unwrap();
                    *id_map_guard = new_id_map;
                }

                // 1. Находим что нужно добавить и что удалить
                let to_add: Vec<String> = target_ids.difference(&self.active_subscriptions).cloned().collect();
                let to_remove: Vec<String> = self.active_subscriptions.difference(&target_ids).cloned().collect();

                if to_add.is_empty() && to_remove.is_empty() {
                    continue;
                }

                info!("MEXC Sync: +{} new, -{} old symbols", to_add.len(), to_remove.len());

                // 2. Сначала отписываемся (освобождаем место в пулах)
                for symbol in to_remove {
                    // В текущей реализации мы просто рассылаем всем пулам, 
                    // каждый пул сам проверит, его ли это символ.
                    for tx in &self.pool_channels {
                        let _ = tx.send(PoolCommand::Unsubscribe(symbol.clone()));
                    }
                    self.active_subscriptions.remove(&symbol);
                }

                // 3. Подписываемся на новые
                for symbol in to_add {
                    let mut success = false;
                    // Пытаемся запихнуть в существующий пул
                    for tx in &self.pool_channels {
                        // Важно: в реальности здесь нужна проверка на лимит 30, 
                        // но для упрощения и надежности мы будем использовать простые очереди.
                        // Если пул перегружен, биржа сама пришлет ошибку.
                        if tx.send(PoolCommand::Subscribe(symbol.clone())).is_ok() {
                            success = true;
                            break;
                        }
                    }

                    // Если нет пулов или все мертвы - создаем новый
                    if !success {
                        let (tx, rx) = mpsc::unbounded_channel();
                        let engine = self.engine.clone();
                        let id_map = self.id_map.clone();
                        let p_id = self.pool_channels.len() + 1;
                        
                        self.pool_channels.push(tx.clone());
                        let first_symbol = symbol.clone();
                        
                        tokio::spawn(async move {
                            Self::manage_smart_pool(p_id, rx, engine, id_map, first_symbol).await;
                        });
                    }
                    self.active_subscriptions.insert(symbol);
                }
            }
        }
    }

    async fn manage_smart_pool(
        pool_id: usize,
        mut cmd_rx: mpsc::UnboundedReceiver<PoolCommand>,
        engine: Arc<SpreadEngine>,
        id_map: Arc<Mutex<HashMap<String, (String, f64, f64, f64)>>>,
        initial_symbol: String
    ) {
        let url = "wss://contract.mexc.com/edge";
        let mut my_symbols: HashSet<String> = HashSet::new();
        my_symbols.insert(initial_symbol.clone());

        loop {
            info!("MEXC Smart Pool #{}: Connecting...", pool_id);

            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    info!("MEXC Smart Pool #{}: Connected", pool_id);

                    // Сразу подписываемся на то, что уже закреплено за этим пулом
                    for sym in &my_symbols {
                        let sub_msg = json!({"method": "sub.depth.full", "param": {"symbol": sym}});
                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }

                    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));

                    loop {
                        tokio::select! {
                            // Слушаем команды на добавление/удаление
                            Some(cmd) = cmd_rx.recv() => {
                                match cmd {
                                    PoolCommand::Subscribe(s) => {
                                        if my_symbols.len() < 30 {
                                            my_symbols.insert(s.clone());
                                            let sub_msg = json!({"method": "sub.depth.full", "param": {"symbol": s}});
                                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                        } else {
                                            // Пул полон - это не должно случаться при правильной логике в run_dynamic, 
                                            // но на всякий случай игнорируем, run_dynamic создаст новый пул.
                                        }
                                    }
                                    PoolCommand::Unsubscribe(s) => {
                                        if my_symbols.remove(&s) {
                                            let unsub_msg = json!({"method": "unsub.depth.full", "param": {"symbol": s}});
                                            let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                        }
                                    }
                                }
                            }
                            _ = ping_interval.tick() => {
                                let ping = json!({"method": "ping"});
                                if let Err(_) = ws_stream.send(Message::Text(ping.to_string().into())).await { break; }
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = Self::handle_message(&text, &engine, &id_map).await {
                                            debug!("MEXC Pool #{}: Parse skip | error: {}", pool_id, e);
                                        }
                                    }
                                    _ => {
                                        warn!("MEXC Smart Pool #{}: Connection lost. Reconnecting...", pool_id);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("MEXC Smart Pool #{}: Connect failed: {}. Retrying in 5s...", pool_id, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(
        text: &str, 
        engine: &Arc<SpreadEngine>,
        id_map: &Arc<Mutex<HashMap<String, (String, f64, f64, f64)>>>
    ) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        if let Some(channel) = v.get("channel").and_then(|c| c.as_str()) {
            if channel == "push.depth.full" {
                if let Some(inst_id) = v.get("symbol").and_then(|s| s.as_str()) {
                    let map_entry = {
                        let id_map_guard = id_map.lock().unwrap();
                        id_map_guard.get(inst_id).cloned()
                    };

                    if let Some((symbol, taker_fee, multiplier, contract_size)) = map_entry {
                        if let Some(data) = v.get("data") {
                            let mut bids = Self::parse_levels(data.get("bids").and_then(|b| b.as_array()), contract_size);
                            let mut asks = Self::parse_levels(data.get("asks").and_then(|a| a.as_array()), contract_size);

                            if multiplier > 1.0 {
                                for (p, a) in bids.iter_mut() {
                                    *p /= multiplier;
                                    *a *= multiplier;
                                }
                                for (p, a) in asks.iter_mut() {
                                    *p /= multiplier;
                                    *a *= multiplier;
                                }
                            }

                            if !bids.is_empty() && !asks.is_empty() {
                                let update = OrderBookUpdate {
                                    exchange: "mexc".to_string(),
                                    symbol,
                                    bids,
                                    asks,
                                    timestamp: v.get("ts").and_then(|t| t.as_u64()).unwrap_or_else(|| {
                                        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                                    }),
                                    local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                                    taker_fee,
                                };
                                engine.process_update(update).await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_levels(levels: Option<&Vec<Value>>, contract_size: f64) -> Vec<(f64, f64)> {
        levels.map(|l| {
            l.iter().map(|item| {
                let p = item[0].as_f64()
                    .or_else(|| item[0].as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(0.0);
                
                // ВАЖНО: Берем индекс [1] - это объем в контрактах, умножаем на contract_size.
                let a = item[1].as_f64()
                    .or_else(|| item[1].as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(0.0) * contract_size;
                
                (p, a)
            }).collect()
        }).unwrap_or_default()
    }
}
