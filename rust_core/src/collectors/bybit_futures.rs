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

pub struct BybitFuturesCollector {
    config: AppConfig,
    engine: Arc<SpreadEngine>,
}

impl BybitFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { config, engine }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, Vec<String>>>) {
        let url = "wss://stream.bybit.com/v5/public/linear";
        let mut current_symbols: HashSet<String> = HashSet::new();

        loop {
            info!("Connecting to Bybit Futures WS: {}", url);

            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Connected to Bybit Futures");

                    loop {
                        tokio::select! {
                            // 1. Слушаем сообщения от биржи
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            // Игнорируем ошибки парсинга служебных ответов
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    _ => {
                                        warn!("Bybit WS connection lost. Reconnecting...");
                                        break;
                                    }
                                }
                            }
                            // 2. Слушаем обновления белого списка
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let new_symbols: HashSet<String> = whitelist.iter()
                                    .filter(|(_, exchanges)| exchanges.contains(&"bybit".to_string()))
                                    .map(|(symbol, _)| symbol.clone())
                                    .collect();

                                if new_symbols == current_symbols {
                                    continue;
                                }

                                // Вычисляем разницу
                                let to_subscribe: Vec<String> = new_symbols.difference(&current_symbols)
                                    .map(|s| format!("orderbook.50.{}", s.replace("/", "")))
                                    .collect();
                                
                                let to_unsubscribe: Vec<String> = current_symbols.difference(&new_symbols)
                                    .map(|s| format!("orderbook.50.{}", s.replace("/", "")))
                                    .collect();

                                // Команды для Bybit V5
                                if !to_subscribe.is_empty() {
                                    info!("Bybit: Subscribing to {} new symbols", to_subscribe.len());
                                    let sub_msg = json!({
                                        "op": "subscribe",
                                        "args": to_subscribe
                                    });
                                    let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                }

                                if !to_unsubscribe.is_empty() {
                                    info!("Bybit: Unsubscribing from {} symbols", to_unsubscribe.len());
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
                    error!("Failed to connect to Bybit: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;

        if let Some(data) = v.get("data") {
            if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
                let symbol = topic.split('.').last().unwrap_or("unknown")
                    .to_uppercase()
                    .replace("/", ""); // Убираем / для унификации
                
                let bids = self.parse_levels(data.get("b").and_then(|b| b.as_array()));
                let asks = self.parse_levels(data.get("a").and_then(|a| a.as_array()));

                if !bids.is_empty() && !asks.is_empty() {
                    let update = OrderBookUpdate {
                        exchange: "bybit".to_string(),
                        symbol,
                        bids,
                        asks,
                        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                    };

                    // Отправляем данные в движок
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
