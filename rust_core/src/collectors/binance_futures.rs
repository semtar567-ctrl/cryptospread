use crate::models::OrderBookUpdate;
use crate::config::AppConfig;
use crate::engine::SpreadEngine;
use std::sync::Arc;
use std::collections::HashMap;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, error, warn};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct BinanceFuturesCollector {
    config: AppConfig,
    engine: Arc<SpreadEngine>,
}

impl BinanceFuturesCollector {
    pub fn new(config: AppConfig, engine: Arc<SpreadEngine>) -> Self {
        Self { config, engine }
    }

    pub async fn run_dynamic(&mut self, whitelist_rx: &mut tokio::sync::broadcast::Receiver<HashMap<String, Vec<String>>>) {
        let mut current_symbols: std::collections::HashSet<String> = std::collections::HashSet::new();
        let url = "wss://fstream.binance.com/ws"; // Используем базовый URL для динамических подписок

        loop {
            info!("Connecting to Binance Futures WS: {}", url);
            
            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Connected to Binance Futures");

                    loop {
                        tokio::select! {
                            // 1. Слушаем сообщения от биржи
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Err(e) = self.handle_message(&text).await {
                                            // Игнорируем ошибки парсинга служебных сообщений (результаты подписки и т.д.)
                                            // error!("Error handling message: {}", e);
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
                            // 2. Слушаем обновления белого списка (раз в минуту)
                            Ok(whitelist) = whitelist_rx.recv() => {
                                let new_symbols: std::collections::HashSet<String> = whitelist.iter()
                                    .filter(|(_, exchanges)| exchanges.contains(&"binance".to_string()))
                                    .map(|(symbol, _)| symbol.clone())
                                    .collect();

                                if new_symbols == current_symbols {
                                    continue;
                                }

                                // Вычисляем что добавить, а что удалить
                                let to_subscribe: Vec<String> = new_symbols.difference(&current_symbols)
                                    .map(|s| s.replace("/", "").to_lowercase() + "@depth20@100ms")
                                    .collect();
                                
                                let to_unsubscribe: Vec<String> = current_symbols.difference(&new_symbols)
                                    .map(|s| s.replace("/", "").to_lowercase() + "@depth20@100ms")
                                    .collect();

                                // Отправляем команды в сокет
                                if !to_subscribe.is_empty() {
                                    info!("Binance: Subscribing to {} new symbols", to_subscribe.len());
                                    let sub_msg = serde_json::json!({
                                        "method": "SUBSCRIBE",
                                        "params": to_subscribe,
                                        "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
                                    });
                                    let _ = ws_stream.send(Message::Text(sub_msg.to_string().into())).await;
                                }

                                if !to_unsubscribe.is_empty() {
                                    info!("Binance: Unsubscribing from {} symbols", to_unsubscribe.len());
                                    let unsub_msg = serde_json::json!({
                                        "method": "UNSUBSCRIBE",
                                        "params": to_unsubscribe,
                                        "id": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 1
                                    });
                                    let _ = ws_stream.send(Message::Text(unsub_msg.to_string().into())).await;
                                }

                                current_symbols = new_symbols;
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

    fn get_current_map_for_exchange(&self, symbols: &[String]) -> HashMap<String, Vec<String>> {
        let mut map = HashMap::new();
        for s in symbols {
            map.insert(s.clone(), vec!["binance".to_string()]);
        }
        map
    }

    async fn connect_and_listen(&self, url: &str) {
        match connect_async(url).await {
            Ok((mut ws_stream, _)) => {
                info!("Connected to Binance Futures");
                while let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Err(e) = self.handle_message(&text).await {
                                error!("Error handling message: {}", e);
                            }
                        }
                        Ok(Message::Ping(p)) => {
                            let _ = ws_stream.send(Message::Pong(p)).await;
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to Binance: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

    async fn handle_message(&self, text: &str) -> anyhow::Result<()> {
        let v: Value = serde_json::from_str(text)?;
        
        if let Some(data) = v.get("data") {
            let symbol = v["stream"].as_str().unwrap_or("unknown")
                .split('@').next().unwrap_or("unknown")
                .to_uppercase()
                .replace("/", ""); // Убираем / для унификации
            
            let bids = self.parse_levels(data.get("b").and_then(|b| b.as_array()));
            let asks = self.parse_levels(data.get("a").and_then(|a| a.as_array()));
            
            let update = OrderBookUpdate {
                exchange: "binance".to_string(),
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
