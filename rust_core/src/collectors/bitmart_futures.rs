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
use flate2::read::{DeflateDecoder, GzDecoder, ZlibDecoder};

pub struct BitmartFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., btcusdt) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl BitmartFuturesCollector {
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
        let ws_url = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1";

        loop {
            info!("Connecting to BitMart Futures WS: {}", ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to BitMart Futures");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        let to_subscribe_args: Vec<String> = current_ids.iter()
                            .map(|id| format!("futures/depth5:{}", id.to_uppercase()))
                            .collect();
                        
                        for chunk in to_subscribe_args.chunks(50) {
                            let sub_msg = serde_json::json!({
                                "op": "subscribe",
                                "args": chunk
                            });
                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                        }
                    }

                    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(15));

                    loop {
                        tokio::select! {
                            _ = ping_interval.tick() => {
                                let ping_msg = serde_json::json!({"op": "ping"});
                                let _ = ws_stream.send(Message::Text(ping_msg.to_string().into())).await;
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Binary(bin))) => {
                                        let mut text = String::new();
                                        let decompressed = DeflateDecoder::new(&bin[..]).read_to_string(&mut text).is_ok()
                                            || ZlibDecoder::new(&bin[..]).read_to_string(&mut text).is_ok()
                                            || GzDecoder::new(&bin[..]).read_to_string(&mut text).is_ok()
                                            || std::str::from_utf8(&bin).map(|s| text = s.to_string()).is_ok();

                                        if decompressed {
                                            if text == "pong" || text.contains("\"pong\"") {
                                                // Pong frame received
                                            } else if text == "ping" || text.contains("\"ping\"") {
                                                let _ = ws_stream.send(Message::Text("pong".into())).await;
                                            } else {
                                                if let Err(e) = self.handle_message(&text).await {
                                                    if !text.contains("subscribe") && !text.contains("errorCode") {
                                                        warn!("BitMart parse error: {} | Raw: {}", e, text);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Text(text))) => {
                                        if text == "pong" || text.contains("\"pong\"") {
                                            // Pong received
                                        } else if text == "ping" || text.contains("\"ping\"") {
                                            let _ = ws_stream.send(Message::Text("pong".into())).await;
                                        } else {
                                            if let Err(e) = self.handle_message(&text).await {
                                                if !text.contains("subscribe") && !text.contains("errorCode") {
                                                    warn!("BitMart parse error: {} | Raw: {}", e, text);
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("BitMart WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("bitmart") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let stream_id = ex_id.to_lowercase();
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0006);
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

                                let to_subscribe_args: Vec<String> = new_ids.difference(&current_ids)
                                    .map(|id| format!("futures/depth5:{}", id.to_uppercase()))
                                    .collect();

                                let to_unsubscribe_args: Vec<String> = current_ids.difference(&new_ids)
                                    .map(|id| format!("futures/depth5:{}", id.to_uppercase()))
                                    .collect();

                                if !to_subscribe_args.is_empty() {
                                    info!("BitMart: Subscribing to {} symbols", to_subscribe_args.len());
                                    for chunk in to_subscribe_args.chunks(50) {
                                        let sub_msg = serde_json::json!({
                                            "op": "subscribe",
                                            "args": chunk
                                        });
                                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                    }
                                }

                                if !to_unsubscribe_args.is_empty() {
                                    info!("BitMart: Unsubscribing from {} symbols", to_unsubscribe_args.len());
                                    for chunk in to_unsubscribe_args.chunks(50) {
                                        let unsub_msg = serde_json::json!({
                                            "op": "unsubscribe",
                                            "args": chunk
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
                    error!("Failed to connect to BitMart WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        // Check group or table for futures/depth
        let group_or_table = v.get("group").and_then(|g| g.as_str())
            .or_else(|| v.get("table").and_then(|t| t.as_str()));

        let is_depth = group_or_table.map_or(false, |gt| gt.contains("futures/depth"));
        if !is_depth {
            return Ok(());
        }

        let data_obj = v.get("data");

        // Extract stream symbol ID
        let stream_id = if let Some(s) = data_obj.and_then(|d| d.get("symbol").or_else(|| d.get("s"))).and_then(|s| s.as_str()) {
            s.to_lowercase()
        } else if let Some(gt) = group_or_table {
            if let Some(pos) = gt.find(':') {
                gt[pos + 1..].to_lowercase()
            } else {
                return Ok(());
            }
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

        let items: Vec<&Value> = if let Some(arr) = data_obj.and_then(|d| d.as_array()) {
            arr.iter().collect()
        } else if let Some(data) = data_obj {
            vec![data]
        } else {
            vec![&v]
        };

        for data in items {
            let raw_bids = data.get("bids").or_else(|| data.get("b")).and_then(|b| b.as_array());
            let raw_asks = data.get("asks").or_else(|| data.get("a")).and_then(|a| a.as_array());

            if raw_bids.is_none() || raw_asks.is_none() {
                continue;
            }

            let bids = self.parse_levels(raw_bids, multiplier, contract_size);
            let asks = self.parse_levels(raw_asks, multiplier, contract_size);

            if !bids.is_empty() && !asks.is_empty() {
                if unified_symbol == "BTCUSDT" {
                    let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert(Instant::now() - std::time::Duration::from_secs(60));
                    if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                        info!("[BITMART OK] {} | Bid: {:.2} | Ask: {:.2}", 
                            unified_symbol, bids[0].0, asks[0].0);
                        *log_timer = Instant::now();
                    }
                }

                let timestamp = data.get("ts").and_then(|t| t.as_u64())
                    .or_else(|| data.get("ts").and_then(|t| t.as_str()).and_then(|s| s.parse::<u64>().ok()))
                    .or_else(|| data.get("ms_t").and_then(|t| t.as_u64()))
                    .or_else(|| data.get("ms_t").and_then(|t| t.as_str()).and_then(|s| s.parse::<u64>().ok()))
                    .unwrap_or_else(|| {
                        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                    });

                let update = OrderBookUpdate {
                    exchange: "bitmart".to_string(),
                    symbol: unified_symbol.clone(),
                    bids,
                    asks,
                    timestamp,
                    local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                    taker_fee,
                };
                self.engine.process_update(update).await?;
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
                        let p = parse_num(&arr[0]).unwrap_or(0.0);
                        let s = parse_num(&arr[1]).unwrap_or(0.0);
                        (p, s)
                    } else {
                        continue;
                    }
                } else if let Some(obj) = level.as_object() {
                    let p = obj.get("price")
                        .or_else(|| obj.get("p"))
                        .and_then(parse_num)
                        .unwrap_or(0.0);
                    let s = obj.get("vol")
                        .or_else(|| obj.get("amount"))
                        .or_else(|| obj.get("a"))
                        .or_else(|| obj.get("v"))
                        .or_else(|| obj.get("qty"))
                        .and_then(parse_num)
                        .unwrap_or(0.0);
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

fn parse_num(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        Some(n)
    } else if let Some(s) = v.as_str() {
        s.parse::<f64>().ok()
    } else if let Some(i) = v.as_i64() {
        Some(i as f64)
    } else {
        None
    }
}
