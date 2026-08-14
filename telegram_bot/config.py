from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    telegram_bot_token: str
    redis_host: str = "localhost"
    redis_port: int = 6379
    database_url: str = "sqlite+aiosqlite:///./cryptospread.db"
    log_level: str = "INFO"
    spread_chart_url: str = "https://happening-rica-plains-hose.trycloudflare.com"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
