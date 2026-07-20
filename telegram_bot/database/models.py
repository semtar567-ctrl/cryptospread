from datetime import datetime
from sqlalchemy import BigInteger, Boolean, Float, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)  # Telegram ID
    username: Mapped[str | None] = mapped_column(String)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    settings: Mapped[list["UserSetting"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    exchanges: Mapped[list["UserExchange"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    blacklist: Mapped[list["UserBlacklist"]] = relationship(back_populates="user", cascade="all, delete-orphan")

class UserSetting(Base):
    __tablename__ = "user_settings"

    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.user_id"), primary_key=True)
    trade_type: Mapped[str] = mapped_column(String, primary_key=True)  # 'futures_futures', 'spot_futures', 'funding'
    
    min_spread_percent: Mapped[float] = mapped_column(Float, default=0.1)
    min_volume_24h: Mapped[float] = mapped_column(Float, default=1000000.0)
    working_volume_usd: Mapped[float] = mapped_column(Float, default=1000.0)
    min_lifetime_seconds: Mapped[int] = mapped_column(Integer, default=3)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    # Переключатели фильтров
    filter_spread_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    filter_volume_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    filter_slippage_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    filter_lifetime_enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    user: Mapped["User"] = relationship(back_populates="settings")

class UserExchange(Base):
    __tablename__ = "user_exchanges"

    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.user_id"), primary_key=True)
    exchange_id: Mapped[str] = mapped_column(String, primary_key=True)  # 'binance', 'bybit', 'okx'
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    user: Mapped["User"] = relationship(back_populates="exchanges")

class TrackedSpread(Base):
    __tablename__ = "tracked_spreads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.user_id"))
    symbol: Mapped[str] = mapped_column(String)
    buy_exchange: Mapped[str] = mapped_column(String)
    sell_exchange: Mapped[str] = mapped_column(String)
    entry_spread: Mapped[float] = mapped_column(Float)
    target_convergence: Mapped[float] = mapped_column(Float, default=0.05)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

class UserBlacklist(Base):
    __tablename__ = "user_blacklist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.user_id"))
    symbol: Mapped[str] = mapped_column(String)

    user: Mapped["User"] = relationship(back_populates="blacklist")
