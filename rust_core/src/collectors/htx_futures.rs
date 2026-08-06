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
use std::io::Read;
use flate2::read::GzDecoder;

pub struct HtxFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., BTC-USDT) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl HtxFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { 
            _config: config, 
            engine,
            id_map: DashMap::new(),
            last_log_time: DashMap::new(),
        }
    }

    pub async fn run_dynamic(
        &mut self, 
        whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>
    ) {
        let mut current_ids: HashSet<String> = HashSet::new();
        let ws_url = "wss://api.hbdm.com/linear-swap-ws";

        loop {
            info!("Connecting to HTX Futures WS: {}", ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to HTX Futures");

                    if !current_ids.is_empty() {
                        let current_vec: Vec<String> = current_ids.iter().cloned().collect();
                        for chunk in current_vec.chunks(30) {
                            for symbol in chunk {
                                let sub_msg = serde_json::json!({
                                    "sub": format!("market.{}.depth.step0", symbol),
                                    "id": symbol
                                });
                                let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                            }
                        }
                    }

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Binary(bin))) => {
                                        let mut text = String::new();
                                        let mut decoder = GzDecoder::new(&bin[..]);
                                        let decompressed = if decoder.read_to_string(&mut text).is_ok() {
                                            true
                                        } else {
                                            text.clear();
                                            std::str::from_utf8(&bin).map(|s| text.push_str(s)).is_ok()
                                        };

                                        if decompressed {
                                            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                                if let Some(ping_val) = v.get("ping") {
                                                    let pong_msg = serde_json::json!({ "pong": ping_val });
                                                    let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                                } else {
                                                    if let Err(e) = self.handle_message(&v).await {
                                                        if !text.contains("\"status\":\"ok\"") && !text.contains("\"subbed\"") {
                                                            warn!("HTX parse error: {} | Raw: {}", e, text);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Text(text))) => {
                                        if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                            if let Some(ping_val) = v.get("ping") {
                                                let pong_msg = serde_json::json!({ "pong": ping_val });
                                                let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                            } else {
                                                if let Err(e) = self.handle_message(&v).await {
                                                    if !text.contains("\"status\":\"ok\"") && !text.contains("\"subbed\"") {
                                                        warn!("HTX parse error: {} | Raw: {}", e, text);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    Some(Ok(Message::Pong(_))) => {}
                                    Some(Ok(Message::Frame(_))) => {}
                                    None => {
                                        warn!("HTX WS stream closed. Reconnecting in 1s...");
                                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        warn!("HTX WS error: {}. Reconnecting in 1s...", e);
                                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("htx") {
                                        let raw_id = ex_info.get("id")
                                            .and_then(|id| id.as_str())
                                            .or_else(|| ex_info.as_str())
                                            .unwrap_or(unified.as_str());

                                        let stream_id = raw_id.to_string();
                                        let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0005);
                                        let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                                        let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).unwrap_or(1.0);

                                        new_id_map.insert(stream_id.clone(), (unified.clone(), fee, multiplier, contract_size));
                                        new_ids.insert(stream_id);
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
                                    info!("HTX: Subscribing to {} symbols", to_subscribe_ids.len());
                                    for chunk in to_subscribe_ids.chunks(30) {
                                        for symbol in chunk {
                                            let sub_msg = serde_json::json!({
                                                "sub": format!("market.{}.depth.step0", symbol),
                                                "id": symbol
                                            });
                                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                        }
                                    }
                                }

                                if !to_unsubscribe_ids.is_empty() {
                                    info!("HTX: Unsubscribing from {} symbols", to_unsubscribe_ids.len());
                                    for chunk in to_unsubscribe_ids.chunks(30) {
                                        for symbol in chunk {
                                            let unsub_msg = serde_json::json!({
                                                "unsub": format!("market.{}.depth.step0", symbol),
                                                "id": symbol
                                            });
                                            let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                        }
                                    }
                                }

                                current_ids = new_ids;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to HTX WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, v: &Value) -> anyhow::Result<()> {
        let ch = match v.get("ch").and_then(|c| c.as_str()) {
            Some(ch) => ch,
            None => return Ok(()),
        };

        // Channel format: "market.BTC-USDT.depth.step0"
        let stream_id = match ch.split('.').nth(1) {
            Some(id) => id,
            None => return Ok(()),
        };

        let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(stream_id) {
            Some(item) => item.value().clone(),
            None => return Ok(()),
        };

        let tick = match v.get("tick") {
            Some(t) => t,
            None => return Ok(()),
        };

        let bids = self.parse_levels(tick.get("bids").and_then(|b| b.as_array()), contract_size, multiplier);
        let asks = self.parse_levels(tick.get("asks").and_then(|a| a.as_array()), contract_size, multiplier);

        if !bids.is_empty() && !asks.is_empty() {
            if unified_symbol == "BTCUSDT" {
                let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - std::time::Duration::from_secs(60));
                if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                    info!("[HTX OK] {} | Bid: {:.2} | Ask: {:.2}", 
                        unified_symbol, bids[0].0, asks[0].0);
                    *log_timer = Instant::now();
                }
            }

            let ts = tick.get("ts")
                .and_then(|t| t.as_u64())
                .unwrap_or_else(|| {
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                });

            let update = OrderBookUpdate {
                exchange: "htx".to_string(),
                symbol: unified_symbol,
                bids,
                asks,
                timestamp: ts,
                local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                taker_fee,
            };

            self.engine.process_update(update).await?;
        }

        Ok(())
    }

    fn parse_levels(&self, levels: Option<&Vec<Value>>, contract_size: f64, multiplier: f64) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        if let Some(arr) = levels {
            for item in arr.iter().take(20) {
                if let Some(pair) = item.as_array() {
                    if pair.len() >= 2 {
                        let mut price = match &pair[0] {
                            Value::Number(n) => n.as_f64().unwrap_or(0.0),
                            Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                            _ => 0.0,
                        };

                        let raw_qty = match &pair[1] {
                            Value::Number(n) => n.as_f64().unwrap_or(0.0),
                            Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                            _ => 0.0,
                        };

                        if price > 0.0 && raw_qty > 0.0 {
                            if multiplier > 1.0 {
                                price /= multiplier;
                            }
                            let qty = raw_qty * contract_size * multiplier;
                            result.push((price, qty));
                        }
                    }
                }
            }
        }
        result
    }
}
