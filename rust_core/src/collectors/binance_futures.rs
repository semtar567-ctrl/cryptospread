use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, error, warn};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct BinanceFuturesCollector {
    config: AppConfig,
    engine: Arc<SpreadEngine>,
    // Mapping: ExchangeID (e.g., btcusdt) -> (Unified (e.g., BTCUSDT), Taker Fee, Multiplier)
    id_map: Mutex<HashMap<String, (String, f64, f64)>>,
}


impl BinanceFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { 
            config, 
            engine,
            id_map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, HashMap<String, serde_json::Value>>>) {
        let mut current_ids: HashSet<String> = HashSet::new();
        // Используем /stream для поддержки Combined Streams (data/stream wrapper)
        let url = "wss://fstream.binance.com/stream";

        loop {
            info!("Connecting to Binance Futures WS: {}", url);
            
            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Connected to Binance Futures");
                    current_ids.clear();

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            if !text.contains("result") {
                                                warn!("Binance parse error: {} | Raw: {}", e, text);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("Binance WS connection lost. Reconnecting...");
                                        break; 
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let mut new_id_map = HashMap::new();
                                let mut new_ids = HashSet::new();

                                for (unified, mapping) in whitelist.iter() {
                                    if let Some(ex_info) = mapping.get("binance") {
                                        if let Some(ex_id) = ex_info.get("id").and_then(|id| id.as_str()).or_else(|| ex_info.as_str()) {
                                            let binance_stream_id = ex_id.to_lowercase();
                                            let fee = ex_info.get("fee").and_then(|f| f.as_f64()).unwrap_or(0.0006);
                                            let multiplier = ex_info.get("multiplier").and_then(|m| m.as_f64()).unwrap_or(1.0);
                                            new_id_map.insert(binance_stream_id.clone(), (unified.clone(), fee, multiplier));
                                            new_ids.insert(binance_stream_id);
                                        }
                                    }
                                }

                                if new_ids == current_ids {
                                    continue;
                                }

                                {
                                    let mut id_map = self.id_map.lock().unwrap();
                                    *id_map = new_id_map;
                                }

                                let to_subscribe: Vec<String> = new_ids.difference(&current_ids)
                                    .map(|id| format!("{}@depth20@100ms", id))
                                    .collect();
                                
                                let to_unsubscribe: Vec<String> = current_ids.difference(&new_ids)
                                    .map(|id| format!("{}@depth20@100ms", id))
                                    .collect();

                                if !to_subscribe.is_empty() {
                                    info!("Binance: Subscribing to {} new streams", to_subscribe.len());
                                    let sub_msg = serde_json::json!({
                                        "method": "SUBSCRIBE",
                                        "params": to_subscribe,
                                        "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
                                    });
                                    let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                }

                                if !to_unsubscribe.is_empty() {
                                    info!("Binance: Unsubscribing from {} streams", to_unsubscribe.len());
                                    let unsub_msg = serde_json::json!({
                                        "method": "UNSUBSCRIBE",
                                        "params": to_unsubscribe,
                                        "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 1
                                    });
                                    let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                }

                                current_ids = new_ids;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Binance: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;
        
        if let Some(data) = v.get("data") {
            if let Some(stream) = v.get("stream").and_then(|s| s.as_str()) {
                let binance_id = stream.split('@').next().unwrap_or("unknown");
                
                let map_entry = {
                    let id_map = self.id_map.lock().unwrap();
                    id_map.get(binance_id).cloned()
                };

                if let Some((symbol, taker_fee, multiplier)) = map_entry {
                    let mut bids = self.parse_levels(data.get("b").and_then(|b| b.as_array()));
                    let mut asks = self.parse_levels(data.get("a").and_then(|a| a.as_array()));
                    
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

                    let update = OrderBookUpdate {
                        exchange: "binance".to_string(),
                        symbol,
                        bids,
                        asks,
                        timestamp: data.get("E").and_then(|t| t.as_u64()).unwrap_or(0),
                        local_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                        taker_fee,
                    };

                    if let Err(e) = self.engine.process_update(update).await {
                        error!("Error processing update in engine: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_levels(&self, levels: Option<&Vec<Value>>) -> Vec<(f64, f64)> {
        levels.map(|l| {
            l.iter().map(|item| {
                let p = item[0].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                let a = item[1].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                (p, a)
            }).collect()
        }).unwrap_or_default()
    }
}
