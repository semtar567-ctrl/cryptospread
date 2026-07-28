use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use dashmap::DashMap;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, error, warn};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH, Instant};

pub struct AsterdexFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., btcusdt) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl AsterdexFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { 
            _config: config, 
            engine,
            id_map: DashMap::new(),
            last_log_time: DashMap::new(),
        }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>) {
        let mut current_ids: HashSet<String> = HashSet::new();
        let ws_url = "wss://fstream.asterdex.com/ws";

        loop {
            info!("Connecting to Aster DEX Futures WS: {}", ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to Aster DEX Futures");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        let to_subscribe: Vec<String> = current_ids.iter().map(|id| format!("{}@depth20@100ms", id.to_lowercase())).collect();
                        let sub_msg = serde_json::json!({
                            "method": "SUBSCRIBE",
                            "params": to_subscribe,
                            "id": 1
                        });
                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                    }

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("\"result\":null") && !text.contains("\"result\"") {
                                                warn!("Aster DEX parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("Aster DEX WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("asterdex") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let stream_id = ex_id.to_lowercase();
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0004);
                                            let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                                            let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).unwrap_or(1.0);
                                            new_id_map.insert(stream_id.clone(), (unified.clone(), fee, multiplier, contract_size));
                                            new_ids.insert(stream_id);
                                        }
                                    }
                                }

                                if new_ids == current_ids {
                                    continue;
                                }

                                self.id_map.clear();
                                for (k, v) in new_id_map {
                                    self.id_map.insert(k, v);
                                }

                                let to_subscribe_streams: Vec<String> = new_ids.difference(&current_ids)
                                    .map(|id| format!("{}@depth20@100ms", id.to_lowercase()))
                                    .collect();

                                let to_unsubscribe_streams: Vec<String> = current_ids.difference(&new_ids)
                                    .map(|id| format!("{}@depth20@100ms", id.to_lowercase()))
                                    .collect();

                                if !to_subscribe_streams.is_empty() {
                                    info!("Aster DEX: Subscribing to {} streams", to_subscribe_streams.len());
                                    let sub_msg = serde_json::json!({
                                        "method": "SUBSCRIBE",
                                        "params": to_subscribe_streams,
                                        "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
                                    });
                                    let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                }

                                if !to_unsubscribe_streams.is_empty() {
                                    info!("Aster DEX: Unsubscribing from {} streams", to_unsubscribe_streams.len());
                                    let unsub_msg = serde_json::json!({
                                        "method": "UNSUBSCRIBE",
                                        "params": to_unsubscribe_streams,
                                        "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 + 1
                                    });
                                    let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                }

                                current_ids = new_ids;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Aster DEX WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
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

        let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(&stream_id) {
            Some(item) => item.value().clone(),
            None => return Ok(()),
        };

        let raw_bids = data.get("bids").and_then(|b| b.as_array()).or_else(|| data.get("b").and_then(|b| b.as_array()));
        let raw_asks = data.get("asks").and_then(|a| a.as_array()).or_else(|| data.get("a").and_then(|a| a.as_array()));

        if raw_bids.is_none() || raw_asks.is_none() {
            return Ok(());
        }

        let bids = self.parse_levels(raw_bids, multiplier, contract_size);
        let asks = self.parse_levels(raw_asks, multiplier, contract_size);

        if !bids.is_empty() && !asks.is_empty() {
            if unified_symbol == "BTCUSDT" {
                let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - std::time::Duration::from_secs(60));
                if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
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
            self.engine.process_update(update).await?;
        }

        Ok(())
    }

    fn parse_levels(&self, levels: Option<&Vec<Value>>, multiplier: f64, contract_size: f64) -> Vec<(f64, f64)> {
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
