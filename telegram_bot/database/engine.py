from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from telegram_bot.config import settings
from telegram_bot.database.models import Base

# Создание асинхронного движка
engine = create_async_engine(settings.database_url, echo=False)

# Создание фабрики сессий
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def init_db():
    """Инициализация базы данных: создание таблиц."""
    async with engine.begin() as conn:
        # В SQLite для переименования колонок/изменения таблиц нужны миграции (Alembic),
        # но для начального создания просто создаем все таблицы.
        await conn.run_sync(Base.metadata.create_all)

async def get_db():
    """Зависимость для получения сессии БД."""
    async with AsyncSessionLocal() as session:
        yield session
