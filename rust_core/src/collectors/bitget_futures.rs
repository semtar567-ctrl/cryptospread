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

pub struct BitgetFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., BTCUSDT) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier)
    id_map: DashMap<String, (String, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl BitgetFuturesCollector {
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
            let ws_url = "wss://ws.bitget.com/v2/ws/public";
            info!("Connecting to Bitget Futures WS: {}", ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to Bitget Futures");

                    if !current_ids.is_empty() {
                        let to_subscribe: Vec<serde_json::Value> = current_ids.iter()
                            .map(|id| serde_json::json!({
                                "instType": "USDT-FUTURES",
                                "channel": "books5",
                                "instId": id
                            }))
                            .collect();

                        let sub_msg = serde_json::json!({
                            "op": "subscribe",
                            "args": to_subscribe
                        });
                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                    }

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("subscribe") {
                                                warn!("Bitget parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("Bitget WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("bitget") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0006);
                                            let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                                            new_id_map.insert(ex_id.to_string(), (unified.clone(), fee, multiplier));
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
                                    info!("Bitget: Subscribing to {} symbols", to_subscribe_ids.len());
                                    for chunk in to_subscribe_ids.chunks(50) {
                                        let args: Vec<serde_json::Value> = chunk.iter().map(|id| serde_json::json!({
                                            "instType": "USDT-FUTURES",
                                            "channel": "books5",
                                            "instId": id
                                        })).collect();

                                        let sub_msg = serde_json::json!({
                                            "op": "subscribe",
                                            "args": args
                                        });
                                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                    }
                                }

                                if !to_unsubscribe_ids.is_empty() {
                                    info!("Bitget: Unsubscribing from {} symbols", to_unsubscribe_ids.len());
                                    for chunk in to_unsubscribe_ids.chunks(50) {
                                        let args: Vec<serde_json::Value> = chunk.iter().map(|id| serde_json::json!({
                                            "instType": "USDT-FUTURES",
                                            "channel": "books5",
                                            "instId": id
                                        })).collect();

                                        let unsub_msg = serde_json::json!({
                                            "op": "unsubscribe",
                                            "args": args
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
                    error!("Failed to connect to Bitget: {}. Retrying in 5s...", e);
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
                    let (unified_symbol, taker_fee, multiplier) = match self.id_map.get(inst_id) {
                        Some(item) => item.value().clone(),
                        None => return Ok(()),
                    };

                    if let Some(data_array) = v.get("data").and_then(|d| d.as_array()) {
                        if let Some(data) = data_array.first() {
                            let mut bids = self.parse_levels(data.get("bids").and_then(|b| b.as_array()));
                            let mut asks = self.parse_levels(data.get("asks").and_then(|a| a.as_array()));

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
                                        info!("[BITGET OK] {} | Bid: {:.2} | Ask: {:.2}", 
                                            unified_symbol, bids[0].0, asks[0].0);
                                        *log_timer = Instant::now();
                                    }
                                }

                                let update = OrderBookUpdate {
                                    exchange: "bitget".to_string(),
                                    symbol: unified_symbol,
                                    bids,
                                    asks,
                                    timestamp: data.get("ts").and_then(|t| t.as_str()).and_then(|s| s.parse::<u64>().ok()).unwrap_or_else(|| {
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
            }
        }

        Ok(())
    }

    fn parse_levels(&self, levels: Option<&Vec<Value>>) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        if let Some(list) = levels {
            for level in list {
                if let Some(arr) = level.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0].as_str().and_then(|p| p.parse::<f64>().ok()).unwrap_or(0.0);
                        let amount = arr[1].as_str().and_then(|a| a.parse::<f64>().ok()).unwrap_or(0.0);
                        if price > 0.0 && amount > 0.0 {
                            result.push((price, amount));
                        }
                    }
                }
            }
        }
        result
    }
}
