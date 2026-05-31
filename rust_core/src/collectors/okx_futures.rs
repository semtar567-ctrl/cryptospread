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

pub struct OkxFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., BTC-USDT-SWAP) -> Unified (e.g., BTCUSDT)
    id_map: DashMap<String, String>,
    // Таймер для контрольного лога
    last_log_time: DashMap<String, Instant>,
}

impl OkxFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { 
            _config: config, 
            engine,
            id_map: DashMap::new(),
            last_log_time: DashMap::new(),
        }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<std::collections::HashMap<String, std::collections::HashMap<String, serde_json::Value>>>) {
        let url_str = "wss://ws.okx.com:8443/ws/v5/public";
        let mut current_ids: HashSet<String> = HashSet::new();

        loop {
            info!("Connecting to OKX Futures WS: {}", url_str);

            match connect_async(url_str).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to OKX Futures");

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if text.contains("\"event\":\"subscribe\"") {
                                            info!("OKX System Message: {}", text);
                                        }
                                        if let Err(e) = self.handle_message(&text).await {
                                            debug!("OKX parse error: {}", e);
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("OKX WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = std::collections::HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("okx") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            new_id_map.insert(ex_id.to_string(), unified.clone());
                                            new_ids.insert(ex_id.to_string());
                                        }
                                    }
                                }

                                if new_ids == current_ids {
                                    continue;
                                }

                                info!("OKX: Whitelist changed ({} total). Updating subscriptions...", new_ids.len());

                                self.id_map.clear();
                                for (k, v) in new_id_map {
                                    self.id_map.insert(k, v);
                                }

                                let to_subscribe: Vec<Value> = new_ids.difference(&current_ids)
                                    .map(|id| json!({"channel": "books5", "instId": id}))
                                    .collect();
                                
                                let to_unsubscribe: Vec<Value> = current_ids.difference(&new_ids)
                                    .map(|id| json!({"channel": "books5", "instId": id}))
                                    .collect();

                                if !to_subscribe.is_empty() {
                                    info!("OKX: Subscribing to {} symbols", to_subscribe.len());
                                    for sub_chunk in to_subscribe.chunks(10) {
                                        let sub_msg = json!({
                                            "op": "subscribe",
                                            "args": sub_chunk
                                        });
                                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string().into())).await {
                                            error!("OKX: Subscription failed: {}", e);
                                        }
                                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                    }
                                }

                                if !to_unsubscribe.is_empty() {
                                    for unsub_chunk in to_unsubscribe.chunks(10) {
                                        let unsub_msg = json!({
                                            "op": "unsubscribe",
                                            "args": unsub_chunk
                                        });
                                        let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                    }
                                }

                                current_ids = new_ids;
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
                                if let Err(e) = ws_stream.send(Message::Text("ping".into())).await {
                                    warn!("OKX: Ping failed: {}. Reconnecting...", e);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("OKX: Connect failed: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        if let Some(arg) = v.get("arg") {
            if arg.get("channel").and_then(|c| c.as_str()) == Some("books5") {
                if let Some(inst_id) = arg.get("instId").and_then(|i| i.as_str()) {
                    let unified_symbol = match self.id_map.get(inst_id) {
                        Some(s) => s.clone(),
                        None => return Ok(()),
                    };

                    if let Some(data_array) = v.get("data").and_then(|d| d.as_array()) {
                        if let Some(data) = data_array.first() {
                            let bids = self.parse_levels(data.get("bids").and_then(|b| b.as_array()));
                            let asks = self.parse_levels(data.get("asks").and_then(|a| a.as_array()));

                            if !bids.is_empty() && !asks.is_empty() {
                                // Контрольный лог для BTC
                                if unified_symbol == "BTCUSDT" {
                                    let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - std::time::Duration::from_secs(60));
                                    if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                                        info!("[OKX OK] {} | Bid: {:.2} | Ask: {:.2}", 
                                            unified_symbol, bids[0].0, asks[0].0);
                                        *log_timer = Instant::now();
                                    }
                                }

                                let update = OrderBookUpdate {
                                    exchange: "okx".to_string(),
                                    symbol: unified_symbol,
                                    bids,
                                    asks,
                                    timestamp: data.get("ts").and_then(|t| t.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0),
                                    local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                                };

                                self.engine.process_update(update).await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
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
