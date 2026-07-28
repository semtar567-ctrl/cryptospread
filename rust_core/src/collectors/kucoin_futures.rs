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
use reqwest::Client;

pub struct KucoinFuturesCollector {
    _config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., XBTUSDTM) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier, ContractSize)
    id_map: DashMap<String, (String, f64, f64, f64)>,
    // Log cooldown timer per symbol
    last_log_time: DashMap<String, Instant>,
}

impl KucoinFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { 
            _config: config, 
            engine,
            id_map: DashMap::new(),
            last_log_time: DashMap::new(),
        }
    }

    async fn get_bullet_token(&self) -> anyhow::Result<(String, String)> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        let resp: Value = client.post("https://api-futures.kucoin.com/api/v1/bullet-public")
            .send()
            .await?
            .json()
            .await?;

        let token = resp["data"]["token"].as_str().ok_or_else(|| anyhow::anyhow!("Missing token in KuCoin response"))?;
        let endpoint = resp["data"]["instanceServers"][0]["endpoint"].as_str().ok_or_else(|| anyhow::anyhow!("Missing endpoint in KuCoin response"))?;

        let ws_url = format!("{}?token={}", endpoint, token);
        Ok((ws_url, token.to_string()))
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>) {
        let mut current_ids: HashSet<String> = HashSet::new();

        loop {
            info!("Fetching KuCoin Futures bullet token...");
            let (ws_url, _token) = match self.get_bullet_token().await {
                Ok(tuple) => tuple,
                Err(e) => {
                    error!("Failed to fetch KuCoin bullet token: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            info!("Connecting to KuCoin Futures WS: {}", ws_url);

            match connect_async(&ws_url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Successfully connected to KuCoin Futures");

                    // Re-subscribe if reconnecting
                    if !current_ids.is_empty() {
                        let ids_vec: Vec<String> = current_ids.iter().cloned().collect();
                        for chunk in ids_vec.chunks(100) {
                            let topic_list = chunk.join(",");
                            let sub_msg = serde_json::json!({
                                "id": 1,
                                "type": "subscribe",
                                "topic": format!("/contractMarket/level2Depth5:{}", topic_list),
                                "response": true
                            });
                            let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                        }
                    }

                    let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(15));

                    loop {
                        tokio::select! {
                            _ = ping_interval.tick() => {
                                let ping_msg = serde_json::json!({
                                    "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().to_string(),
                                    "type": "ping"
                                });
                                let _ = ws_stream.send(Message::Text(ping_msg.to_string().into())).await;
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("\"welcome\"") && !text.contains("\"ack\"") && !text.contains("\"pong\"") {
                                                warn!("KuCoin parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("KuCoin WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("kucoinfutures") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0006);
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
                                    info!("KuCoin: Subscribing to {} symbols", to_subscribe_ids.len());
                                    for chunk in to_subscribe_ids.chunks(100) {
                                        let topic_list = chunk.join(",");
                                        let sub_msg = serde_json::json!({
                                            "id": 1,
                                            "type": "subscribe",
                                            "topic": format!("/contractMarket/level2Depth5:{}", topic_list),
                                            "response": true
                                        });
                                        let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                    }
                                }

                                if !to_unsubscribe_ids.is_empty() {
                                    info!("KuCoin: Unsubscribing from {} symbols", to_unsubscribe_ids.len());
                                    for chunk in to_unsubscribe_ids.chunks(100) {
                                        let topic_list = chunk.join(",");
                                        let unsub_msg = serde_json::json!({
                                            "id": 2,
                                            "type": "unsubscribe",
                                            "topic": format!("/contractMarket/level2Depth5:{}", topic_list),
                                            "response": true
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
                    error!("Failed to connect to KuCoin: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        if let Some(msg_type) = v.get("type").and_then(|t| t.as_str()) {
            if msg_type == "message" {
                if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
                    if topic.contains("/contractMarket/level2Depth5:") {
                        let inst_id = topic.split(':').nth(1).unwrap_or("");
                        let (unified_symbol, taker_fee, multiplier, contract_size) = match self.id_map.get(inst_id) {
                            Some(item) => item.value().clone(),
                            None => return Ok(()),
                        };

                        if let Some(data) = v.get("data") {
                            let mut bids = self.parse_levels(data.get("bids").and_then(|b| b.as_array()), contract_size);
                            let mut asks = self.parse_levels(data.get("asks").and_then(|a| a.as_array()), contract_size);

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
                                        info!("[KUCOIN OK] {} | Bid: {:.2} | Ask: {:.2}", 
                                            unified_symbol, bids[0].0, asks[0].0);
                                        *log_timer = Instant::now();
                                    }
                                }

                                let update = OrderBookUpdate {
                                    exchange: "kucoinfutures".to_string(),
                                    symbol: unified_symbol,
                                    bids,
                                    asks,
                                    timestamp: v.get("sn").and_then(|t| t.as_u64()).unwrap_or_else(|| {
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

    fn parse_levels(&self, levels: Option<&Vec<Value>>, contract_size: f64) -> Vec<(f64, f64)> {
        let mut result = Vec::new();
        if let Some(list) = levels {
            for level in list {
                if let Some(arr) = level.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0].as_str().and_then(|p| p.parse::<f64>().ok())
                            .or_else(|| arr[0].as_f64()).unwrap_or(0.0);
                        let raw_size = arr[1].as_str().and_then(|a| a.parse::<f64>().ok())
                            .or_else(|| arr[1].as_f64()).unwrap_or(0.0);
                        let amount = raw_size * contract_size;

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
