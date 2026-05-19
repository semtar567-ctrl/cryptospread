use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, error, warn};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct OkxFuturesCollector {
    config: AppConfig,
    engine: Arc<SpreadEngine>,
}

impl OkxFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { config, engine }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, Vec<String>>>) {
        let url = "wss://ws.okx.com:8443/ws/v5/public";
        let mut current_symbols: HashSet<String> = HashSet::new();

        loop {
            info!("Connecting to OKX Futures WS: {}", url);

            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Connected to OKX Futures");

                    loop {
                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            // Игнорируем ошибки парсинга
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("OKX WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let new_symbols: HashSet<String> = whitelist.iter()
                                    .filter(|(_, exchanges)| exchanges.contains(&"okx".to_string()))
                                    .map(|(symbol, _)| symbol.clone())
                                    .collect();

                                if new_symbols == current_symbols {
                                    continue;
                                }

                                // Format for OKX: BTC-USDT-SWAP
                                let format_symbol = |s: &str| -> String {
                                    s.replace("/", "-").to_uppercase() + "-SWAP"
                                };

                                let to_subscribe: Vec<Value> = new_symbols.difference(&current_symbols)
                                    .map(|s| json!({"channel": "books5", "instId": format_symbol(s)}))
                                    .collect();
                                
                                let to_unsubscribe: Vec<Value> = current_symbols.difference(&new_symbols)
                                    .map(|s| json!({"channel": "books5", "instId": format_symbol(s)}))
                                    .collect();

                                if !to_subscribe.is_empty() {
                                    info!("OKX: Subscribing to {} new symbols", to_subscribe.len());
                                    let sub_msg = json!({
                                        "op": "subscribe",
                                        "args": to_subscribe
                                    });
                                    let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                }

                                if !to_unsubscribe.is_empty() {
                                    info!("OKX: Unsubscribing from {} symbols", to_unsubscribe.len());
                                    let unsub_msg = json!({
                                        "op": "unsubscribe",
                                        "args": to_unsubscribe
                                    });
                                    let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                }

                                current_symbols = new_symbols;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to OKX: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        // Ожидаем {"arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"}, "data": [...]}
        if let Some(arg) = v.get("arg") {
            if arg.get("channel").and_then(|c| c.as_str()) == Some("books5") {
                if let Some(inst_id) = arg.get("instId").and_then(|i| i.as_str()) {
                    if let Some(data_array) = v.get("data").and_then(|d| d.as_array()) {
                        if let Some(data) = data_array.first() {
                            // Convert BTC-USDT-SWAP back to BTCUSDT
                            let parts: Vec<&str> = inst_id.split('-').collect();
                            if parts.len() >= 2 {
                                let symbol = format!("{}{}", parts[0], parts[1]); // BTCUSDT format
                                
                                let bids = self.parse_levels(data.get("bids").and_then(|b| b.as_array()));
                                let asks = self.parse_levels(data.get("asks").and_then(|a| a.as_array()));

                                if !bids.is_empty() && !asks.is_empty() {
                                    let update = OrderBookUpdate {
                                        exchange: "okx".to_string(),
                                        symbol,
                                        bids,
                                        asks,
                                        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                                    };

                                    if let Err(e) = self.engine.process_update(update).await {
                                        error!("Error processing OKX update in engine: {}", e);
                                    }
                                }
                            }
                        }
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
                // В OKX объем передается в контрактах, но для упрощения пока парсим как есть. 
                // CCXT Volume monitor отдает объемы в базовой валюте, OKX отдает контракты (например 1 контракт = 0.01 BTC).
                // Для чистого MVP мы пока берем сырые значения, но в будущем понадобится нормализация объемов.
                // В books5 API значения приходят как строки.
                (p, a)
            }).collect()
        }).unwrap_or_default()
    }
}
