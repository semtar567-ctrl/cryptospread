use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, error, warn, debug};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use tokio::sync::mpsc;
use dashmap::DashMap;

/// Команда для управления пулом соединений
enum PoolCommand {
    Subscribe(String),
    Unsubscribe(String),
}

pub struct AsterdexFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Маппинг: ExchangeID (e.g., btcusdt) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, Contract Size)
    id_map: Arc<Mutex<HashMap<String, (String, f64, f64, f64)>>>,
    // Список всех монет, на которые мы СЕЙЧАС подписаны (по всем пулам)
    active_subscriptions: HashSet<String>,
    // Каналы связи с каждым запущенным пулом
    pool_channels: Vec<mpsc::UnboundedSender<PoolCommand>>,
}

impl AsterdexFuturesCollector {
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
        info!("Aster DEX Collector 2.0 (Smart Pooling) started");
        let last_log_time = Arc::new(DashMap::<String, Instant>::new());

        loop {
            if let Ok(whitelist) = whitelist_rx.recv().await {
                let mut new_id_map = HashMap::new();
                let mut target_ids = HashSet::new();

                for (unified, mapping) in whitelist.iter() {
                    if let Some(ex_info) = mapping.get("asterdex") {
                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0004);
                            let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                            let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).unwrap_or(1.0);

                            let stream_id = ex_id.replace("/", "").replace("-", "").to_lowercase();
                            new_id_map.insert(stream_id.clone(), (unified.clone(), fee, multiplier, contract_size));
                            target_ids.insert(stream_id);
                        }
                    }
                }

                // Обновляем глобальную карту ID
                {
                    let mut id_map_guard = self.id_map.lock().unwrap();
                    *id_map_guard = new_id_map;
                }

                let to_add: Vec<String> = target_ids.difference(&self.active_subscriptions).cloned().collect();
                let to_remove: Vec<String> = self.active_subscriptions.difference(&target_ids).cloned().collect();

                if to_add.is_empty() && to_remove.is_empty() {
                    continue;
                }

                info!("Aster DEX Sync: +{} new, -{} old symbols across pools", to_add.len(), to_remove.len());

                for symbol in to_remove {
                    for tx in &self.pool_channels {
                        let _ = tx.send(PoolCommand::Unsubscribe(symbol.clone()));
                    }
                    self.active_subscriptions.remove(&symbol);
                }

                for symbol in to_add {
                    let mut success = false;
                    for tx in &self.pool_channels {
                        if tx.send(PoolCommand::Subscribe(symbol.clone())).is_ok() {
                            success = true;
                            break;
                        }
                    }

                    if !success {
                        let (tx, rx) = mpsc::unbounded_channel();
                        let engine = self.engine.clone();
                        let id_map = self.id_map.clone();
                        let log_time = last_log_time.clone();
                        let p_id = self.pool_channels.len() + 1;
                        
                        self.pool_channels.push(tx.clone());
                        let first_symbol = symbol.clone();
                        
                        tokio::spawn(async move {
                            Self::manage_smart_pool(p_id, rx, engine, id_map, log_time, first_symbol).await;
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
        last_log_time: Arc<DashMap<String, Instant>>,
        initial_symbol: String
    ) {
        let ws_url = "wss://fstream.asterdex.com/ws";
        let mut my_symbols: HashSet<String> = HashSet::new();
        my_symbols.insert(initial_symbol.clone());

        loop {
            info!("Aster DEX Smart Pool #{}: Connecting to {}...", pool_id, ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Aster DEX Smart Pool #{}: Connected", pool_id);

                    // Сразу подписываемся на закрепленные за этим пулом символы
                    let streams: Vec<String> = my_symbols.iter().map(|s| format!("{}@depth20@100ms", s)).collect();
                    for (idx, chunk) in streams.chunks(25).enumerate() {
                        let sub_msg = json!({
                            "method": "SUBSCRIBE",
                            "params": chunk,
                            "id": idx + 1
                        });
                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }

                    loop {
                        tokio::select! {
                            Some(cmd) = cmd_rx.recv() => {
                                match cmd {
                                    PoolCommand::Subscribe(s) => {
                                        if my_symbols.len() < 150 {
                                            my_symbols.insert(s.clone());
                                            let stream_name = format!("{}@depth20@100ms", s);
                                            let sub_msg = json!({
                                                "method": "SUBSCRIBE",
                                                "params": [stream_name],
                                                "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
                                            });
                                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                        }
                                    }
                                    PoolCommand::Unsubscribe(s) => {
                                        if my_symbols.remove(&s) {
                                            let stream_name = format!("{}@depth20@100ms", s);
                                            let unsub_msg = json!({
                                                "method": "UNSUBSCRIBE",
                                                "params": [stream_name],
                                                "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
                                            });
                                            let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                        }
                                    }
                                }
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = Self::handle_message(&text, &engine, &id_map, &last_log_time).await {
                                            if !text.contains("\"result\":null") && !text.contains("\"result\"") {
                                                debug!("Aster DEX Pool #{}: Parse error: {} | Raw: {}", pool_id, e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    Some(Ok(Message::Pong(_))) => {}
                                    Some(Ok(Message::Frame(_))) => {}
                                    Some(Ok(Message::Binary(_))) => {}
                                    _ => {
                                        warn!("Aster DEX Smart Pool #{}: Connection lost. Reconnecting in 1s...", pool_id);
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Aster DEX Smart Pool #{}: Connect failed: {}. Retrying in 5s...", pool_id, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(
        text: &str, 
        engine: &Arc<SpreadEngine>,
        id_map: &Arc<Mutex<HashMap<String, (String, f64, f64, f64)>>>,
        last_log_time: &Arc<DashMap<String, Instant>>,
    ) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        let (stream_name, data) = if let Some(stream) = v.get("stream").and_then(|s| s.as_str()) {
            (Some(stream), v.get("data").unwrap_or(&v))
        } else {
            (None, &v)
        };

        let stream_id = if let Some(s) = stream_name {
            s.split('@').next().unwrap_or("").to_lowercase()
        } else if let Some(s) = data.get("s").and_then(|s| s.as_str()).or_else(|| data.get("symbol").and_then(|s| s.as_str())) {
            s.to_lowercase()
        } else {
            return Ok(());
        };

        if stream_id.is_empty() {
            return Ok(());
        }

        let map_entry = {
            let id_map_guard = id_map.lock().unwrap();
            id_map_guard.get(&stream_id).cloned()
        };

        let (unified_symbol, taker_fee, multiplier, contract_size) = match map_entry {
            Some(item) => item,
            None => return Ok(()),
        };

        let raw_bids = data.get("bids").and_then(|b| b.as_array()).or_else(|| data.get("b").and_then(|b| b.as_array()));
        let raw_asks = data.get("asks").and_then(|a| a.as_array()).or_else(|| data.get("a").and_then(|a| a.as_array()));

        if raw_bids.is_none() || raw_asks.is_none() {
            return Ok(());
        }

        let bids = Self::parse_levels(raw_bids, multiplier, contract_size);
        let asks = Self::parse_levels(raw_asks, multiplier, contract_size);

        if !bids.is_empty() && !asks.is_empty() {
            if unified_symbol == "BTCUSDT" {
                let mut log_timer = last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - Duration::from_secs(60));
                if log_timer.elapsed() >= Duration::from_secs(10) {
                    info!("[ASTERDEX OK] {} | Bid: {:.2} | Ask: {:.2}", 
                        unified_symbol, bids[0].0, asks[0].0);
                    *log_timer = Instant::now();
                }
            }

            let timestamp = data.get("E").and_then(|t| t.as_u64())
                .or_else(|| data.get("T").and_then(|t| t.as_u64()))
                .or_else(|| data.get("time").and_then(|t| t.as_u64()))
                .unwrap_or_else(|| {
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                });

            let update = OrderBookUpdate {
                exchange: "asterdex".to_string(),
                symbol: unified_symbol,
                bids,
                asks,
                timestamp,
                local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                taker_fee,
            };
            engine.process_update(update).await?;
        }

        Ok(())
    }

    fn parse_levels(levels: Option<&Vec<Value>>, multiplier: f64, contract_size: f64) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        if let Some(list) = levels {
            for level in list {
                let (raw_price, raw_sz) = if let Some(arr) = level.as_array() {
                    if arr.len() >= 2 {
                        let p = arr[0].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| arr[0].as_f64()).unwrap_or(0.0);
                        let s = arr[1].as_str().and_then(|v| v.parse::<f64>().ok()).or_else(|| arr[1].as_f64()).unwrap_or(0.0);
                        (p, s)
                    } else {
                        continue;
                    }
                } else if let Some(obj) = level.as_object() {
                    let p = obj.get("price").or_else(|| obj.get("p")).and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let s = obj.get("amount").or_else(|| obj.get("qty")).or_else(|| obj.get("a")).or_else(|| obj.get("v")).and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    (p, s)
                } else {
                    continue;
                };

                let mut price = raw_price;
                let mut amount = raw_sz * contract_size;

                if multiplier != 1.0 {
                    price /= multiplier;
                    amount *= multiplier;
                }

                if price > 0.0 && amount > 0.0 {
                    result.push((price, amount));
                }
            }
        }
        result
    }
}
