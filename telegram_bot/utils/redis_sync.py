import json
from redis.asyncio import Redis
from telegram_bot.config import settings

_redis_client = None

def get_redis_client():
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    return _redis_client

async def close_redis_client():
    global _redis_client
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception: pass
        _redis_client = None

async def sync_user_settings_to_redis(user_id: int, session=None):
    """
    Уведомляет Rust-ядро и Signal Engine об обновлении настроек пользователя через Redis Pub/Sub.
    """
    redis = get_redis_client()
    try:
        await redis.publish("user_updates", str(user_id))
    except Exception: pass

async def sync_all_users_to_redis(session=None):
    """
    Уведомляет Rust-ядро и Signal Engine о необходимости полной синхронизации всех пользователей.
    """
    redis = get_redis_client()
    try:
        await redis.publish("user_updates", "ALL")
    except Exception: pass
