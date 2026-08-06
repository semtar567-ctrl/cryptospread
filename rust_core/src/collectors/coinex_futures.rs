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

pub struct CoinexFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: Exchange symbol (e.g., BTCUSDT) -> (Unified symbol (e.g., BTCUSDT), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl CoinexFuturesCollector {
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
        whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>,
    ) {
        let mut current_ids: HashSet<String> = HashSet::new();
        let ws_url = "wss://socket.coinex.com/v2/futures";

        loop {
            info!("Connecting to CoinEx Futures WS: {}", ws_url);

            match connect_async(ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to CoinEx Futures WS");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        let current_vec: Vec<String> = current_ids.iter().cloned().collect();
                        for chunk in current_vec.chunks(30) {
                            let market_list: Vec<serde_json::Value> = chunk
                                .iter()
                                .map(|symbol| serde_json::json!([symbol, 50, "0", true]))
                                .collect();
                            let sub_msg = serde_json::json!({
                                "method": "depth.subscribe",
                                "params": {
                                    "market_list": market_list
                                },
                                "id": 1
                            });
                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                        }
                    }

                    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(15));

                    loop {
                        tokio::select! {
                            _ = ping_interval.tick() => {
                                let ping_msg = serde_json::json!({
                                    "method": "server.ping",
                                    "params": {},
                                    "id": 1
                                });
                                let _ = ws_stream.send(Message::Text(ping_msg.to_string().into())).await;
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        let trimmed = text.trim();
                                        if trimmed == "ping" || trimmed == "Ping" {
                                            let pong_msg = serde_json::json!({
                                                "method": "server.pong",
                                                "params": {},
                                                "id": 1
                                            });
                                            let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                        } else if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                            if v.get("method").and_then(|m| m.as_str()) == Some("server.ping") {
                                                let pong_msg = serde_json::json!({
                                                    "method": "server.pong",
                                                    "params": {},
                                                    "id": 1
                                                });
                                                let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                            } else {
                                                if let Err(e) = self.handle_message(&v).await {
                                                    if !text.contains("depth.subscribe") && !text.contains("server.pong") {
                                                        warn!("CoinEx Futures parse error: {} | Raw: {}", e, text);
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
                                            let trimmed = text.trim();
                                            if trimmed == "ping" || trimmed == "Ping" {
                                                let pong_msg = serde_json::json!({
                                                    "method": "server.pong",
                                                    "params": {},
                                                    "id": 1
                                                });
                                                let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                            } else if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                                if v.get("method").and_then(|m| m.as_str()) == Some("server.ping") {
                                                    let pong_msg = serde_json::json!({
                                                        "method": "server.pong",
                                                        "params": {},
                                                        "id": 1
                                                    });
                                                    let _ = ws_stream.send(Message::Text(pong_msg.to_string().into())).await;
                                                } else {
                                                    if let Err(e) = self.handle_message(&v).await {
                                                        if !text.contains("depth.subscribe") && !text.contains("server.pong") {
                                                            warn!("CoinEx Futures parse error: {} | Raw: {}", e, text);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("CoinEx Futures WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            res = whitelist_rx.recv() => {
                                let whitelist = match res {
                                    Ok(w) => w,
                                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                                };
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("coinexfutures").or_else(|| mapping.get("coinex")) {
                                        let raw_id = ex_info.get("exchange_id_symbol")
                                            .and_then(|id| id.as_str())
                                            .or_else(|| ex_info.get("id").and_then(|id| id.as_str()))
                                            .or_else(|| ex_info.as_str())
                                            .unwrap_or(unified.as_str());

                                        let symbol = raw_id.to_uppercase().replace("-", "").replace("_", "").replace("/", "");
                                        let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0005);
                                        let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                                        let contract_size = ex_info.get("contract_size").and_then(|c| c.as_f64()).unwrap_or(1.0);
                                        new_id_map.insert(symbol.clone(), (unified.clone(), fee, multiplier, contract_size));
                                        new_ids.insert(symbol);
                                    }
                                }

                                self.id_map.clear();
                                for (k, v) in new_id_map {
                                    self.id_map.insert(k, v);
                                }

                                if new_ids == current_ids {
                                    continue;
                                }

                                let to_subscribe: Vec<String> = new_ids.difference(&current_ids).cloned().collect();
                                let to_unsubscribe: Vec<String> = current_ids.difference(&new_ids).cloned().collect();

                                if !to_subscribe.is_empty() {
                                    info!("CoinEx Futures: Subscribing to {} symbols", to_subscribe.len());
                                    for chunk in to_subscribe.chunks(30) {
                                        let market_list: Vec<serde_json::Value> = chunk
                                            .iter()
                                            .map(|symbol| serde_json::json!([symbol, 50, "0", true]))
                                            .collect();
                                        let sub_msg = serde_json::json!({
                                            "method": "depth.subscribe",
                                            "params": {
                                                "market_list": market_list
                                            },
                                            "id": 1
                                        });
                                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                    }
                                }

                                if !to_unsubscribe.is_empty() {
                                    info!("CoinEx Futures: Unsubscribing from {} symbols", to_unsubscribe.len());
                                    for chunk in to_unsubscribe.chunks(30) {
                                        let market_list: Vec<serde_json::Value> = chunk
                                            .iter()
                                            .map(|symbol| serde_json::json!([symbol, 50, "0", true]))
                                            .collect();
                                        let unsub_msg = serde_json::json!({
                                            "method": "depth.unsubscribe",
                                            "params": {
                                                "market_list": market_list
                                            },
                                            "id": 1
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
                    error!("Failed to connect to CoinEx Futures WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, v: &Value) -> anyhow::Result<()> {
        let method = match v.get("method").and_then(|m| m.as_str()) {
            Some(m) => m,
            None => return Ok(()),
        };

        if method != "depth.update" {
            return Ok(());
        }

        let data = match v.get("data") {
            Some(d) => d,
            None => return Ok(()),
        };

        let market = match data.get("market").and_then(|m| m.as_str()) {
            Some(m) => m.to_uppercase(),
            None => return Ok(()),
        };

        let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(&market) {
            Some(item) => item.value().clone(),
            None => return Ok(()),
        };

        let depth_obj = data.get("depth").unwrap_or(data);
        let raw_bids = depth_obj.get("bids").and_then(|b| b.as_array());
        let raw_asks = depth_obj.get("asks").and_then(|a| a.as_array());

        if raw_bids.is_none() || raw_asks.is_none() {
            return Ok(());
        }

        let bids = self.parse_levels(raw_bids, multiplier, contract_size);
        let asks = self.parse_levels(raw_asks, multiplier, contract_size);

        if !bids.is_empty() && !asks.is_empty() {
            if unified_symbol == "BTCUSDT" {
                let mut log_timer = self
                    .last_log_time
                    .entry(unified_symbol.clone())
                    .or_insert_with(|| Instant::now() - std::time::Duration::from_secs(60));
                if log_timer.elapsed() >= std::time::Duration::from_secs(10) {
                    info!(
                        "[COINEX OK] {} | Bid: {:.2} | Ask: {:.2}",
                        unified_symbol, bids[0].0, asks[0].0
                    );
                    *log_timer = Instant::now();
                }
            }

            let timestamp = depth_obj
                .get("updated_at")
                .and_then(|t| t.as_u64())
                .or_else(|| depth_obj.get("ts").and_then(|t| t.as_u64()))
                .or_else(|| data.get("updated_at").and_then(|t| t.as_u64()))
                .or_else(|| data.get("ts").and_then(|t| t.as_u64()))
                .unwrap_or_else(|| {
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                });

            let update = OrderBookUpdate {
                exchange: "coinexfutures".to_string(),
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

    fn parse_levels(
        &self,
        levels: Option<&Vec<Value>>,
        multiplier: f64,
        contract_size: f64,
    ) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        let mult = if multiplier <= 0.0 { 1.0 } else { multiplier };
        if let Some(list) = levels {
            for level in list {
                if let Some(arr) = level.as_array() {
                    if arr.len() >= 2 {
                        let raw_p = parse_num(&arr[0]).unwrap_or(0.0);
                        let raw_a = parse_num(&arr[1]).unwrap_or(0.0);
                        if raw_p > 0.0 && raw_a > 0.0 {
                            let price = raw_p / mult;
                            let amount = raw_a * mult * contract_size;
                            result.push((price, amount));
                        }
                    }
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
