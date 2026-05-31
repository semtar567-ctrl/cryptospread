import json
from redis.asyncio import Redis
from telegram_bot.config import settings
from telegram_bot.database.models import User, UserSetting, UserExchange
from sqlalchemy import select
from sqlalchemy.orm import selectinload

async def sync_user_settings_to_redis(user_id: int, session):
    """
    Уведомляет Rust-ядро об обновлении настроек пользователя через Redis Pub/Sub.
    """
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    try:
        # Просто публикуем ID пользователя в канал обновлений
        await redis.publish("user_updates", str(user_id))
    finally:
        await redis.aclose()

async def sync_all_users_to_redis(session):
    """
    Уведомляет Rust-ядро о необходимости полной синхронизации всех пользователей.
    """
    redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    try:
        await redis.publish("user_updates", "ALL")
    finally:
        await redis.aclose()
