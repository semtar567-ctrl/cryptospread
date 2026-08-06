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

pub struct LbankFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: Exchange symbol (e.g., btc_usdt) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl LbankFuturesCollector {
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
        let ws_url = "wss://www.lbkex.net/ws/V2/";

        loop {
            info!("Connecting to LBank Futures WS: {}", ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to LBank Futures WS");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        let current_vec: Vec<String> = current_ids.iter().cloned().collect();
                        for chunk in current_vec.chunks(30) {
                            for symbol in chunk {
                                let sub_msg = serde_json::json!({
                                    "action": "subscribe",
                                    "subscribe": "depth",
                                    "pair": symbol,
                                    "depth": "100"
                                });
                                let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        }
                    }

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                            if let Some(ping_val) = v.get("ping") {
                                                let pong_msg = serde_json::json!({
                                                    "action": "pong",
                                                    "pong": ping_val
                                                });
                                                let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                            } else {
                                                if let Err(e) = self.handle_message(&v).await {
                                                    if !text.contains("subscribe") && !text.contains("pong") {
                                                        warn!("LBank Futures parse error: {} | Raw: {}", e, text);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Binary(bin))) => {
                                        let mut text = String::new();
                                        let decompressed = (GzDecoder::new(&bin[..]).read_to_string(&mut text).is_ok() && !text.is_empty())
                                            || { text.clear(); DeflateDecoder::new(&bin[..]).read_to_string(&mut text).is_ok() && !text.is_empty() }
                                            || { text.clear(); ZlibDecoder::new(&bin[..]).read_to_string(&mut text).is_ok() && !text.is_empty() }
                                            || { text.clear(); std::str::from_utf8(&bin).map(|s| text = s.to_string()).is_ok() };

                                        if decompressed {
                                            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                                if let Some(ping_val) = v.get("ping") {
                                                    let pong_msg = serde_json::json!({
                                                        "action": "pong",
                                                        "pong": ping_val
                                                    });
                                                    let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                                } else {
                                                    if let Err(e) = self.handle_message(&v).await {
                                                        if !text.contains("subscribe") && !text.contains("pong") {
                                                            warn!("LBank Futures parse error: {} | Raw: {}", e, text);
                                                        }
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
                                        warn!("LBank Futures WS stream closed. Reconnecting in 1s...");
                                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        warn!("LBank Futures WS error: {}. Reconnecting in 1s...", e);
                                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            res = whitelist_rx.recv() => {
                                let whitelist = match res {
                                    Ok(w) => w,
                                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                                };
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("lbank") {
                                        let raw_id = ex_info.get("exchange_id_symbol")
                                            .and_then(|id| id.as_str())
                                            .or_else(|| ex_info.get("id").and_then(|id| id.as_str()))
                                            .or_else(|| ex_info.as_str())
                                            .unwrap_or(unified.as_str());

                                        let stream_id = format_lbank_symbol(raw_id);
                                        let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0006);
                                        let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                                        let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).unwrap_or(1.0);

                                        new_id_map.insert(stream_id.clone(), (unified.clone(), fee, multiplier, contract_size));
                                        new_ids.insert(stream_id);
                                    }
                                }

                                self.id_map.clear();
                                for (k, v) in new_id_map {
                                    self.id_map.insert(k, v);
                                }

                                if new_ids == current_ids {
                                    continue;
                                }

                                let to_subscribe_ids: Vec<String> = new_ids.difference(&current_ids).cloned().collect();
                                let to_unsubscribe_ids: Vec<String> = current_ids.difference(&new_ids).cloned().collect();

                                if !to_subscribe_ids.is_empty() {
                                    info!("LBank Futures: Subscribing to {} symbols", to_subscribe_ids.len());
                                    for chunk in to_subscribe_ids.chunks(30) {
                                        for symbol in chunk {
                                            let sub_msg = serde_json::json!({
                                                "action": "subscribe",
                                                "subscribe": "depth",
                                                "pair": symbol,
                                                "depth": "100"
                                            });
                                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                        }
                                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                                    }
                                }

                                if !to_unsubscribe_ids.is_empty() {
                                    info!("LBank Futures: Unsubscribing from {} symbols", to_unsubscribe_ids.len());
                                    for chunk in to_unsubscribe_ids.chunks(30) {
                                        for symbol in chunk {
                                            let unsub_msg = serde_json::json!({
                                                "action": "unsubscribe",
                                                "subscribe": "depth",
                                                "pair": symbol,
                                                "depth": "100"
                                            });
                                            let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                        }
                                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                                    }
                                }

                                current_ids = new_ids;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to LBank Futures WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, v: &Value) -> anyhow::Result<()> {
        let pair = match v.get("pair").and_then(|p| p.as_str()) {
            Some(p) => p.to_lowercase(),
            None => return Ok(()),
        };

        let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(&pair) {
            Some(item) => item.value().clone(),
            None => return Ok(()),
        };

        let depth_obj = match v.get("depth") {
            Some(d) => d,
            None => return Ok(()),
        };

        let raw_bids = depth_obj.get("bids").and_then(|b| b.as_array());
        let raw_asks = depth_obj.get("asks").and_then(|a| a.as_array());

        if raw_bids.is_none() || raw_asks.is_none() {
            return Ok(());
        }

        let bids = self.parse_levels(raw_bids, multiplier, contract_size);
        let asks = self.parse_levels(raw_asks, multiplier, contract_size);

        if !bids.is_empty() && !asks.is_empty() {
            if unified_symbol == "BTCUSDT" {
                let mut log_timer = self.last_log_time.entry(unified_symbol.clone()).or_insert_with(|| Instant::now() - std::time::Duration::from_secs(60));
                if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                    info!("[LBANK OK] {} | Bid: {:.2} | Ask: {:.2}", 
                        unified_symbol, bids[0].0, asks[0].0);
                    *log_timer = Instant::now();
                }
            }

            let timestamp = v.get("TS").and_then(|t| t.as_u64())
                .or_else(|| v.get("ts").and_then(|t| t.as_u64()))
                .or_else(|| v.get("TS").and_then(|t| t.as_str()).and_then(|s| s.parse::<u64>().ok()))
                .or_else(|| v.get("ts").and_then(|t| t.as_str()).and_then(|s| s.parse::<u64>().ok()))
                .or_else(|| v.get("t").and_then(|t| t.as_u64()))
                .unwrap_or_else(|| {
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                });

            let update = OrderBookUpdate {
                exchange: "lbank".to_string(),
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
        let mult = if multiplier == 0.0 { 1.0 } else { multiplier };

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

                let price = raw_price / mult;
                let amount = raw_sz * mult * contract_size;

                if price > 0.0 && amount > 0.0 {
                    result.push((price, amount));
                }
            }
        }
        result
    }
}

fn format_lbank_symbol(raw: &str) -> String {
    let lower = raw.to_lowercase();
    if lower.contains('_') || lower.contains('-') || lower.contains('/') {
        lower.replace('-', "_").replace('/', "_")
    } else {
        if let Some(pos) = lower.rfind("usdt") {
            if pos > 0 {
                return format!("{}_{}", &lower[..pos], &lower[pos..]);
            }
        } else if let Some(pos) = lower.rfind("usdc") {
            if pos > 0 {
                return format!("{}_{}", &lower[..pos], &lower[pos..]);
            }
        } else if let Some(pos) = lower.rfind("usd") {
            if pos > 0 {
                return format!("{}_{}", &lower[..pos], &lower[pos..]);
            }
        }
        lower
    }
}
