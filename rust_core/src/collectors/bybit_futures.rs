use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::Arc;
use std::collections::HashSet;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, error, warn, debug};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use dashmap::DashMap;

pub struct BybitFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., BTCUSDT) -> (Unified (e.g., BTCUSDT), Taker Fee)
    id_map: DashMap<String, (String, f64)>,
    // Хранилище полных стаканов: symbol -> (bids, asks)
    books: DashMap<String, (Vec<(f64, f64)>, Vec<(f64, f64)>)>,
    // Таймер для контрольного лога
    last_log_time: DashMap<String, Instant>,
}

impl BybitFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { 
            _config: config, 
            engine,
            id_map: DashMap::new(),
            books: DashMap::new(),
            last_log_time: DashMap::new(),
        }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<std::collections::HashMap<String, std::collections::HashMap<String, serde_json::Value>>>) {
        let url = "wss://stream.bybit.com/v5/public/linear";
        let mut current_ids: HashSet<String> = HashSet::new();

        loop {
            info!("Connecting to Bybit Futures WS: {}", url);

            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Connected to Bybit Futures");

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("success") && !text.contains("pong") {
                                                warn!("Bybit parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("Bybit WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = std::collections::HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("bybit") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0006);
                                            new_id_map.insert(ex_id.to_string(), (unified.clone(), fee));
                                            new_ids.insert(ex_id.to_string());
                                        }
                                    }
                                }

                                if new_ids == current_ids {
                                    continue;
                                }

                                info!("Bybit: Whitelist changed ({} total). Updating subscriptions...", new_ids.len());
                                
                                self.id_map.clear();
                                for (k, v) in new_id_map {
                                    self.id_map.insert(k, v);
                                }
                                self.books.clear();

                                let to_subscribe: Vec<String> = new_ids.difference(&current_ids)
                                    .map(|id| format!("orderbook.50.{}", id))
                                    .collect();
                                
                                let to_unsubscribe: Vec<String> = current_ids.difference(&new_ids)
                                    .map(|id| format!("orderbook.50.{}", id))
                                    .collect();

                                if !to_subscribe.is_empty() {
                                    info!("Bybit: Subscribing to {} symbols", to_subscribe.len());
                                    for chunk in to_subscribe.chunks(10) {
                                        let sub_msg = json!({ "op": "subscribe", "args": chunk });
                                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string().into())).await {
                                            error!("Bybit: Subscription failed: {}", e);
                                        }
                                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                    }
                                }

                                if !to_unsubscribe.is_empty() {
                                    for chunk in to_unsubscribe.chunks(10) {
                                        let unsub_msg = json!({ "op": "unsubscribe", "args": chunk });
                                        let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                    }
                                }

                                current_ids = new_ids;
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
                                let ping_msg = json!({"op": "ping"});
                                if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string().into())).await {
                                    warn!("Bybit: Ping failed: {}. Reconnecting...", e);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Bybit: Connect failed: {}. Retrying in 5s...", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        let msg_type = match v.get("type").and_then(|t| t.as_str()) {
            Some(t) => t,
            None => return Ok(()),
        };

        let topic = match v.get("topic").and_then(|t| t.as_str()) {
            Some(t) => t,
            None => return Ok(()),
        };

        let bybit_id = topic.split('.').last().unwrap_or("unknown");
        
        let (unified_symbol, taker_fee) = match self.id_map.get(bybit_id) {
            Some(item) => item.value().clone(),
            None => return Ok(()),
        };

        if let Some(data) = v.get("data") {
            let mut current_book = self.books.entry(unified_symbol.clone()).or_insert((Vec::new(), Vec::new()));
            
            if msg_type == "snapshot" {
                current_book.0 = self.parse_levels(data.get("b").and_then(|b| b.as_array()));
                current_book.1 = self.parse_levels(data.get("a").and_then(|a| a.as_array()));
            } else if msg_type == "delta" {
                let b_delta = self.parse_levels(data.get("b").and_then(|b| b.as_array()));
                let a_delta = self.parse_levels(data.get("a").and_then(|a| a.as_array()));
                Self::merge_levels(&mut current_book.0, b_delta, true);
                Self::merge_levels(&mut current_book.1, a_delta, false);
            }

            if !current_book.0.is_empty() && !current_book.1.is_empty() {
                // Копируем данные из DashMap-блокировки, чтобы отправить в движок
                let bids = current_book.0.clone();
                let asks = current_book.1.clone();
                
                // Освобождаем блокировку DashMap (current_book выходит из области видимости ниже)
                drop(current_book);

                // Контрольный лог (раз в 10 сек для каждой монеты, если это BTC)
                if unified_symbol == "BTCUSDT" {
                    let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - std::time::Duration::from_secs(60));
                    if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                        info!("[BYBIT OK] {} | Bid: {:.2} | Ask: {:.2} | Depth: {}", 
                            unified_symbol, bids[0].0, asks[0].0, bids.len());
                        *log_timer = Instant::now();
                    }
                }

                let update = OrderBookUpdate {
                    exchange: "bybit".to_string(),
                    symbol: unified_symbol,
                    bids,
                    asks,
                    timestamp: v.get("ts").and_then(|t| t.as_u64()).unwrap_or(0),
                    local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                    taker_fee,
                };
                self.engine.process_update(update).await?;
            }
        }
        Ok(())
    }

    fn merge_levels(current: &mut Vec<(f64, f64)>, delta: Vec<(f64, f64)>, descending: bool) {
        for (p, a) in delta {
            if a == 0.0 {
                current.retain(|(price, _)| (*price - p).abs() > f64::EPSILON);
            } else {
                if let Some(existing) = current.iter_mut().find(|(price, _)| (*price - p).abs() <= f64::EPSILON) {
                    existing.1 = a;
                } else {
                    current.push((p, a));
                }
            }
        }
        if descending {
            current.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        } else {
            current.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        }
        current.truncate(50);
    }

    fn parse_levels(&self, levels: Option<&Vec<Value>>) -> Vec<(f64, f64)> {
        levels.map(|l| {
            l.iter().map(|item| {
                let p = item[0].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                let a = item[1].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                (p, a)
            }).collect()
        }).unwrap_or_default()
    }
}
