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

pub struct GateioFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., BTC_USDT) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl GateioFuturesCollector {
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

        loop {
            let ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdt";
            info!("Connecting to Gate.io Futures WS: {}", ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to Gate.io Futures");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        for id in current_ids.iter() {
                            let sub_msg = serde_json::json!({
                                "time": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                                "channel": "futures.order_book",
                                "event": "subscribe",
                                "payload": [id, "20", "0"]
                            });
                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                        }
                    }

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("subscribe") {
                                                warn!("Gate.io parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("Gate.io WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("gateio") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0005);
                                            let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                                            let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).unwrap_or(1.0);
                                            new_id_map.insert(ex_id.to_string(), (unified.clone(), fee, multiplier, contract_size));
                                            new_ids.insert(ex_id.to_string());
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

                                let to_subscribe_ids: Vec<String> = new_ids.difference(&current_ids).cloned().collect();
                                let to_unsubscribe_ids: Vec<String> = current_ids.difference(&new_ids).cloned().collect();

                                if !to_subscribe_ids.is_empty() {
                                    info!("Gate.io: Subscribing to {} symbols", to_subscribe_ids.len());
                                    for id in to_subscribe_ids.iter() {
                                        let sub_msg = serde_json::json!({
                                            "time": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                                            "channel": "futures.order_book",
                                            "event": "subscribe",
                                            "payload": [id, "20", "0"]
                                        });
                                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                    }
                                }

                                if !to_unsubscribe_ids.is_empty() {
                                    info!("Gate.io: Unsubscribing from {} symbols", to_unsubscribe_ids.len());
                                    for id in to_unsubscribe_ids.iter() {
                                        let unsub_msg = serde_json::json!({
                                            "time": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                                            "channel": "futures.order_book",
                                            "event": "unsubscribe",
                                            "payload": [id, "20", "0"]
                                        });
                                        let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                    }
                                }

                                current_ids = new_ids;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Gate.io: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        if v.get("channel").and_then(|c| c.as_str()) == Some("futures.order_book") {
            if let Some(result) = v.get("result") {
                if let Some(contract) = result.get("contract").and_then(|c| c.as_str()) {
                    let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(contract) {
                        Some(item) => item.value().clone(),
                        None => return Ok(()),
                    };

                    let mut bids = self.parse_levels(result.get("bids").and_then(|b| b.as_array()), contract_size);
                    let mut asks = self.parse_levels(result.get("asks").and_then(|a| a.as_array()), contract_size);

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
                        if unified_symbol == "BTCUSDT" {
                            let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - std::time::Duration::from_secs(60));
                            if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                                info!("[GATEIO OK] {} | Bid: {:.2} | Ask: {:.2}", 
                                    unified_symbol, bids[0].0, asks[0].0);
                                *log_timer = Instant::now();
                            }
                        }

                        let update = OrderBookUpdate {
                            exchange: "gateio".to_string(),
                            symbol: unified_symbol,
                            bids,
                            asks,
                            timestamp: result.get("t").and_then(|t| t.as_u64()).unwrap_or_else(|| {
                                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                            }),
                            local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                            taker_fee,
                        };
                        self.engine.process_update(update).await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_levels(&self, levels: Option<&Vec<Value>>, contract_size: f64) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        if let Some(list) = levels {
            for level in list {
                if let Some(obj) = level.as_object() {
                    let price = obj.get("p").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    let raw_size = obj.get("s").and_then(|s| s.as_f64().or_else(|| s.as_str().and_then(|str_s| str_s.parse::<f64>().ok()))).unwrap_or(0.0);
                    let amount = raw_size * contract_size;

                    if price > 0.0 && amount > 0.0 {
                        result.push((price, amount));
                    }
                }
            }
        }
        result
    }
}
