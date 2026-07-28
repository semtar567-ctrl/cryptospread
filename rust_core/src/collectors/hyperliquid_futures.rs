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

pub struct HyperliquidFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: Coin ID (e.g. "BTC") -> (Unified Symbol (e.g. "BTCUSDT"), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl HyperliquidFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self {
            _config: config,
            engine,
            id_map: DashMap::new(),
            last_log_time: DashMap::new(),
        }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>) {
        let ws_url = "wss://api.hyperliquid.xyz/ws";
        let mut current_ids: HashSet<String> = HashSet::new();

        loop {
            info!("Connecting to Hyperliquid Futures WS: {}", ws_url);
            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to Hyperliquid Futures");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        for coin in &current_ids {
                            let sub_msg = serde_json::json!({
                                "method": "subscribe",
                                "subscription": {
                                    "type": "l2Book",
                                    "coin": coin
                                }
                            });
                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                        }
                    }

                    let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

                    loop {
                        tokio::select! {
                            _ = ping_interval.tick() => {
                                let ping_msg = serde_json::json!({ "method": "ping" });
                                if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string().into())).await {
                                    warn!("Hyperliquid ping failed: {}. Reconnecting...", e);
                                    break;
                                }
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("\"pong\"") && !text.contains("\"subscriptionResponse\"") {
                                                warn!("Hyperliquid parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("Hyperliquid WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("hyperliquid") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.00035);
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

                                let to_subscribe: Vec<String> = new_ids.difference(&current_ids).cloned().collect();
                                let to_unsubscribe: Vec<String> = current_ids.difference(&new_ids).cloned().collect();

                                if !to_subscribe.is_empty() {
                                    info!("Hyperliquid: Subscribing to {} coins", to_subscribe.len());
                                    for coin in &to_subscribe {
                                        let sub_msg = serde_json::json!({
                                            "method": "subscribe",
                                            "subscription": {
                                                "type": "l2Book",
                                                "coin": coin
                                            }
                                        });
                                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                    }
                                }

                                if !to_unsubscribe.is_empty() {
                                    info!("Hyperliquid: Unsubscribing from {} coins", to_unsubscribe.len());
                                    for coin in &to_unsubscribe {
                                        let unsub_msg = serde_json::json!({
                                            "method": "unsubscribe",
                                            "subscription": {
                                                "type": "l2Book",
                                                "coin": coin
                                            }
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
                    error!("Failed to connect to Hyperliquid WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        let channel = match v.get("channel").and_then(|c| c.as_str()) {
            Some(c) => c,
            None => return Ok(()),
        };

        if channel == "l2Book" {
            if let Some(data) = v.get("data") {
                let coin = match data.get("coin").and_then(|c| c.as_str()) {
                    Some(c) => c,
                    None => return Ok(()),
                };

                let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(coin) {
                    Some(item) => item.value().clone(),
                    None => return Ok(()),
                };

                let (raw_bids, raw_asks) = if let Some(levels) = data.get("levels").and_then(|l| l.as_array()) {
                    if levels.len() >= 2 {
                        (levels[0].as_array(), levels[1].as_array())
                    } else {
                        (None, None)
                    }
                } else {
                    (data.get("bids").and_then(|b| b.as_array()), data.get("asks").and_then(|a| a.as_array()))
                };

                let bids = self.parse_levels(raw_bids, multiplier, contract_size);
                let asks = self.parse_levels(raw_asks, multiplier, contract_size);

                if !bids.is_empty() && !asks.is_empty() {
                    if unified_symbol == "BTCUSDT" {
                        let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - std::time::Duration::from_secs(60));
                        if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                            info!("[HYPERLIQUID OK] {} | Bid: {:.2} | Ask: {:.2}", 
                                unified_symbol, bids[0].0, asks[0].0);
                            *log_timer = Instant::now();
                        }
                    }

                    let timestamp = data.get("time").and_then(|t| t.as_u64()).unwrap_or_else(|| {
                        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                    });

                    let update = OrderBookUpdate {
                        exchange: "hyperliquid".to_string(),
                        symbol: unified_symbol,
                        bids,
                        asks,
                        timestamp,
                        local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                        taker_fee,
                    };
                    self.engine.process_update(update).await?;
                }
            }
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
                    let p = obj.get("px").and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let s = obj.get("sz").and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
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
