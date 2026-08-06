use crate::models::{UserConfig, UserTradeSettings, TrackedSpreadConfig};
use sqlx::{postgres::PgPool, Row};
use anyhow::Result;

pub struct UserManager {
    pool: PgPool,
}

impl UserManager {
    pub async fn new(database_url: &str) -> Result<Self> {
        let clean_url = if let Some(stripped) = database_url.strip_prefix("postgresql+asyncpg://") {
            format!("postgres://{}", stripped)
        } else if let Some(stripped) = database_url.strip_prefix("postgresql://") {
            format!("postgres://{}", stripped)
        } else {
            database_url.to_string()
        };

        let pool = PgPool::connect(&clean_url).await?;
        Ok(Self { pool })
    }

    /// Получает список всех активных пользователей и их настройки из PostgreSQL
    pub async fn get_all_active_users(&self) -> Result<Vec<UserConfig>> {
        // Запрос всех активных пользователей с их настройками фьючерсов
        let rows = sqlx::query(
            r#"
            SELECT 
                u.user_id, 
                u.is_active,
                s.min_spread_percent,
                s.min_volume_24h,
                s.working_volume_usd,
                s.min_lifetime_seconds,
                s.is_enabled as s_enabled,
                s.filter_spread_enabled,
                s.filter_volume_enabled,
                s.filter_slippage_enabled,
                s.filter_lifetime_enabled
            FROM users u
            JOIN user_settings s ON u.user_id = s.user_id
            WHERE u.is_active = true AND s.trade_type = 'futures_futures' AND s.is_enabled = true
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut configs = Vec::new();

        for row in rows {
            let user_id: i64 = row.get("user_id");
            let is_active: bool = row.get("is_active");

            // Получаем список включенных бирж для пользователя
            let exchange_rows = sqlx::query(
                "SELECT exchange_id FROM user_exchanges WHERE user_id = $1 AND is_enabled = true"
            )
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;

            let enabled_exchanges = exchange_rows
                .into_iter()
                .map(|r| r.get::<String, _>("exchange_id"))
                .collect();

            // Получаем список отслеживаемых спредов для пользователя
            let tracked_rows = sqlx::query(
                "SELECT symbol, buy_exchange, sell_exchange, target_convergence::float8 as target_convergence FROM tracked_spreads WHERE user_id = $1 AND is_active = true"
            )
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;

            let tracked_spreads = tracked_rows
                .into_iter()
                .map(|r| TrackedSpreadConfig {
                    symbol: r.get::<String, _>("symbol"),
                    buy_exchange: r.get::<String, _>("buy_exchange"),
                    sell_exchange: r.get::<String, _>("sell_exchange"),
                    target_convergence: r.get::<Option<f64>, _>("target_convergence").unwrap_or(0.05),
                })
                .collect();

            configs.push(UserConfig {
                user_id,
                is_active,
                futures_settings: Some(UserTradeSettings {
                    min_spread_percent: row.get::<Option<f64>, _>("min_spread_percent").unwrap_or(0.1),
                    min_volume_24h: row.get::<Option<f64>, _>("min_volume_24h").unwrap_or(1000000.0),
                    working_volume_usd: row.get::<Option<f64>, _>("working_volume_usd").unwrap_or(1000.0),
                    min_lifetime_seconds: row.get::<Option<i32>, _>("min_lifetime_seconds").unwrap_or(3),
                    is_enabled: if row.get::<Option<bool>, _>("s_enabled").unwrap_or(true) { 1 } else { 0 },
                    filter_spread_enabled: if row.get::<Option<bool>, _>("filter_spread_enabled").unwrap_or(true) { 1 } else { 0 },
                    filter_volume_enabled: if row.get::<Option<bool>, _>("filter_volume_enabled").unwrap_or(true) { 1 } else { 0 },
                    filter_slippage_enabled: if row.get::<Option<bool>, _>("filter_slippage_enabled").unwrap_or(true) { 1 } else { 0 },
                    filter_lifetime_enabled: if row.get::<Option<bool>, _>("filter_lifetime_enabled").unwrap_or(true) { 1 } else { 0 },
                }),
                enabled_exchanges,
                tracked_spreads,
            });
        }

        Ok(configs)
    }
}
