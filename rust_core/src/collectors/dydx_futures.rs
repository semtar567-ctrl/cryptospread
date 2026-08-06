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

pub struct DydxFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: dYdX Market ID (e.g. "BTC-USD") -> (Unified Symbol (e.g. "BTCUSDT"), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Local OrderBook Cache: Unified Symbol -> (Bids, Asks)
    books: DashMap<String, (Vec<(f64, f64)>, Vec<(f64, f64)>)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl DydxFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self {
            _config: config,
            engine,
            id_map: DashMap::new(),
            books: DashMap::new(),
            last_log_time: DashMap::new(),
        }
    }

    pub async fn run_dynamic(
        &mut self,
        whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>,
    ) {
        let ws_url = "wss://indexer.dydx.trade/v4/ws";
        let mut current_ids: HashSet<String> = HashSet::new();

        loop {
            info!("Connecting to dYdX v4 Futures WS: {}", ws_url);
            self.books.clear();

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to dYdX v4 Futures WS");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        let ids_vec: Vec<String> = current_ids.iter().cloned().collect();
                        for chunk in ids_vec.chunks(20) {
                            for id in chunk {
                                let sub_msg = serde_json::json!({
                                    "type": "subscribe",
                                    "channel": "v4_orderbook",
                                    "id": id
                                });
                                let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        }
                    }

                    let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

                    loop {
                        tokio::select! {
                            _ = ping_interval.tick() => {
                                let ping_msg = serde_json::json!({ "type": "ping" });
                                if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string().into())).await {
                                    warn!("dYdX ping failed: {}. Reconnecting...", e);
                                    break;
                                }
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("\"pong\"") && !text.contains("\"connected\"") {
                                                warn!("dYdX parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    Some(Ok(Message::Pong(_))) => {}
                                    Some(Ok(Message::Frame(_))) => {}
                                    Some(Ok(Message::Binary(_))) => {}
                                    None => {
                                        warn!("dYdX WS stream closed. Reconnecting in 1s...");
                                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        warn!("dYdX WS error: {}. Reconnecting in 1s...", e);
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
                                    if let Some(ex_info) = mapping.get("dydx") {
                                        let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0005);
                                        let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).filter(|m| *m > 0.0).unwrap_or(1.0);
                                        let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).filter(|c| *c > 0.0).unwrap_or(1.0);

                                        let raw_id = ex_info.get("exchange_id_symbol").and_then(|id| id.as_str())
                                            .or_else(|| ex_info.get("id").and_then(|id| id.as_str()))
                                            .or_else(|| ex_info.as_str());
                                        let dydx_id = match raw_id {
                                            Some(id) if !id.is_empty() => {
                                                let clean = id.to_uppercase().replace('/', "-");
                                                if clean.ends_with("-USD") {
                                                    clean
                                                } else if clean.ends_with("-USDT") {
                                                    format!("{}-USD", clean.strip_suffix("-USDT").unwrap())
                                                } else if clean.ends_with("USD") {
                                                    format!("{}-USD", clean.strip_suffix("USD").unwrap().trim_end_matches('-'))
                                                } else if clean.ends_with("USDT") {
                                                    format!("{}-USD", clean.strip_suffix("USDT").unwrap().trim_end_matches('-'))
                                                } else {
                                                    format!("{}-USD", clean.trim_end_matches('-'))
                                                }
                                            }
                                            _ => {
                                                let base = unified.to_uppercase().replace("USDT", "").replace("USD", "");
                                                format!("{}-USD", base.trim_matches('-'))
                                            }
                                        };

                                        new_id_map.insert(dydx_id.clone(), (unified.clone(), fee, multiplier, contract_size));
                                        new_ids.insert(dydx_id);
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
                                    info!("dYdX: Subscribing to {} symbols", to_subscribe_ids.len());
                                    for chunk in to_subscribe_ids.chunks(20) {
                                        for id in chunk {
                                            let sub_msg = serde_json::json!({
                                                "type": "subscribe",
                                                "channel": "v4_orderbook",
                                                "id": id
                                            });
                                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                        }
                                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                                    }
                                }

                                if !to_unsubscribe_ids.is_empty() {
                                    info!("dYdX: Unsubscribing from {} symbols", to_unsubscribe_ids.len());
                                    for chunk in to_unsubscribe_ids.chunks(20) {
                                        for id in chunk {
                                            let unsub_msg = serde_json::json!({
                                                "type": "unsubscribe",
                                                "channel": "v4_orderbook",
                                                "id": id
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
                    error!("Failed to connect to dYdX WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
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

        if msg_type != "subscribed" && msg_type != "channel_data" {
            return Ok(());
        }

        let id = match v.get("id").and_then(|i| i.as_str()) {
            Some(i) => i,
            None => return Ok(()),
        };

        let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(id) {
            Some(item) => item.value().clone(),
            None => return Ok(()),
        };

        let contents = v.get("contents").unwrap_or(&v);

        let raw_bids = contents.get("bids").and_then(|b| b.as_array());
        let raw_asks = contents.get("asks").and_then(|a| a.as_array());

        if raw_bids.is_none() && raw_asks.is_none() {
            return Ok(());
        }

        let bids_parsed = self.parse_levels(raw_bids, multiplier, contract_size);
        let asks_parsed = self.parse_levels(raw_asks, multiplier, contract_size);

        let mut book_entry = self.books.entry(unified_symbol.clone()).or_insert((Vec::new(), Vec::new()));

        if msg_type == "subscribed" {
            book_entry.0 = bids_parsed.into_iter().filter(|(_, a)| *a > 0.0).collect();
            book_entry.1 = asks_parsed.into_iter().filter(|(_, a)| *a > 0.0).collect();
        } else if msg_type == "channel_data" {
            Self::merge_levels(&mut book_entry.0, bids_parsed, true);
            Self::merge_levels(&mut book_entry.1, asks_parsed, false);
        }

        if !book_entry.0.is_empty() && !book_entry.1.is_empty() {
            let bids = book_entry.0.clone();
            let asks = book_entry.1.clone();
            drop(book_entry);

            if unified_symbol == "BTCUSDT" {
                let mut log_timer = self
                    .last_log_time
                    .entry(unified_symbol.clone())
                    .or_insert_with(|| Instant::now() - std::time::Duration::from_secs(60));
                if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                    info!(
                        "[DYDX OK] {} | Bid: {:.2} | Ask: {:.2} | Depth: Bids {}, Asks {}",
                        unified_symbol, bids[0].0, asks[0].0, bids.len(), asks.len()
                    );
                    *log_timer = Instant::now();
                }
            }

            let timestamp = contents
                .get("timestamp")
                .or_else(|| contents.get("updatedAt"))
                .or_else(|| v.get("timestamp"))
                .and_then(|t| {
                    t.as_u64()
                        .or_else(|| t.as_str().and_then(|s| s.parse::<u64>().ok()))
                })
                .unwrap_or_else(|| {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64
                });

            let update = OrderBookUpdate {
                exchange: "dydx".to_string(),
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
                let (raw_price, raw_size) = if let Some(arr) = level.as_array() {
                    if arr.len() >= 2 {
                        let p = arr[0]
                            .as_str()
                            .and_then(|s| s.parse::<f64>().ok())
                            .or_else(|| arr[0].as_f64())
                            .unwrap_or(0.0);
                        let s = arr[1]
                            .as_str()
                            .and_then(|v| v.parse::<f64>().ok())
                            .or_else(|| arr[1].as_f64())
                            .unwrap_or(0.0);
                        (p, s)
                    } else {
                        continue;
                    }
                } else if let Some(obj) = level.as_object() {
                    let p = obj
                        .get("price")
                        .or_else(|| obj.get("px"))
                        .or_else(|| obj.get("p"))
                        .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64()))
                        .unwrap_or(0.0);

                    let s = obj
                        .get("size")
                        .or_else(|| obj.get("amount"))
                        .or_else(|| obj.get("sz"))
                        .or_else(|| obj.get("qty"))
                        .or_else(|| obj.get("a"))
                        .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64()))
                        .unwrap_or(0.0);

                    (p, s)
                } else {
                    continue;
                };

                let price = raw_price / multiplier;
                let amount = raw_size * multiplier * contract_size;

                if price > 0.0 && amount >= 0.0 {
                    result.push((price, amount));
                }
            }
        }
        result
    }

    fn merge_levels(current: &mut Vec<(f64, f64)>, delta: Vec<(f64, f64)>, descending: bool) {
        for (p, a) in delta {
            if a == 0.0 {
                current.retain(|(price, _)| (*price - p).abs() > f64::EPSILON);
            } else {
                if let Some(pos) = current.iter().position(|(price, _)| (*price - p).abs() <= f64::EPSILON) {
                    current[pos].1 = a;
                } else {
                    current.push((p, a));
                }
            }
        }

        if descending {
            current.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            current.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        }

        if current.len() > 50 {
            current.truncate(50);
        }
    }
}
