import sys
import asyncio
import json
import time
import socket
import structlog
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from telegram_bot.config import settings
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import User, UserSetting, UserExchange
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiohttp.resolver import ThreadedResolver

# Фикс DNS и Event Loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = structlog.get_logger()

class IPv4Session(AiohttpSession):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Фикс для Windows + VPN: принудительно используем IPv4 и стандартный резолвер
        self._connector_init["resolver"] = ThreadedResolver()
        self._connector_init["family"] = socket.AF_INET

class SignalEngine:
    def __init__(self):
        self.redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
        self.bot = Bot(token=settings.telegram_bot_token, session=IPv4Session())
        # Хранилище для отслеживания времени жизни связки: (symbol, buy_ex, sell_ex) -> first_seen_timestamp
        self.active_spreads = {}

    async def run(self):
        logger.info("Signal Engine started. Listening for market data...")
        
        # Начинаем читать только новые сообщения с момента запуска
        last_id = "$"
        stream_name = getattr(settings, 'redis', {}).get('stream_name', 'market_signals') if isinstance(getattr(settings, 'redis', None), dict) else "market_signals"

        while True:
            try:
                streams = {stream_name: last_id}
                
                response = await self.redis.xread(streams, count=10, block=1000)
                
                for s_name, messages in response:
                    for msg_id, data in messages:
                        raw_payload = data.get("data")
                        if raw_payload:
                            signal_data = json.loads(raw_payload)
                            await self.process_signal(signal_data)
                        
                        # Обновляем ID, чтобы в следующем цикле читать только новые
                        last_id = msg_id

            except Exception as e:
                logger.error("Error in Signal Engine loop", error=str(e))
                await asyncio.sleep(1)

    def calculate_vwap(self, levels, volume_usd):
        """
        Расчет средневзвешенной цены (VWAP) для покупки или продажи на заданный объем USD.
        levels: [[price, amount], ...]
        volume_usd: сумма в долларах
        """
        total_usd = 0.0
        total_qty = 0.0
        
        for price, qty in levels:
            level_usd = price * qty
            if total_usd + level_usd >= volume_usd:
                # Берем только часть этого уровня, чтобы добить до нужного объема
                needed_usd = volume_usd - total_usd
                needed_qty = needed_usd / price
                total_qty += needed_qty
                total_usd += needed_usd
                break
            else:
                total_qty += qty
                total_usd += level_usd
        
        if total_usd < volume_usd:
            # Ликвидности не хватило даже на 20 уровнях
            return None
            
        return total_usd / total_qty

    async def process_signal(self, data):
        symbol = data["symbol"]
        buy_ex = data["buy_on"].lower()
        sell_ex = data["sell_on"].lower()
        raw_spread = data.get("net_spread", 0)

        logger.info("Processing signal from Redis", symbol=symbol, buy_ex=buy_ex, sell_ex=sell_ex, raw_spread=f"{raw_spread:.4f}%")
        
        async with AsyncSessionLocal() as session:
            stmt = select(User).where(User.is_active == True).options(
                selectinload(User.settings),
                selectinload(User.exchanges)
            )
            result = await session.execute(stmt)
            users = result.scalars().all()

            if not users:
                logger.warning("No active users found in DB")
                return

            for user in users:
                user_exchanges = {ex.exchange_id for ex in user.exchanges if ex.is_enabled}
                if buy_ex not in user_exchanges or sell_ex not in user_exchanges:
                    logger.debug("Exchanges not enabled for user", user_id=user.user_id, buy_ex=buy_ex, sell_ex=sell_ex)
                    continue

                user_setting = next((s for s in user.settings if s.trade_type == "futures_futures"), None)
                if not user_setting or not user_setting.is_enabled:
                    logger.debug("Futures-Futures disabled for user", user_id=user.user_id)
                    continue

                # --- РАСЧЕТ ПРОСКАЛЬЗЫВАНИЯ ---
                working_vol = user_setting.working_volume_usd
                
                # На бирже Buy мы ПОКУПАЕМ (смотрим Asks)
                avg_buy_price = self.calculate_vwap(data["buy_book"]["asks"], working_vol)
                # На бирже Sell мы ПРОДАЕМ (смотрим Bids)
                avg_sell_price = self.calculate_vwap(data["sell_book"]["bids"], working_vol)

                if avg_buy_price is None or avg_sell_price is None:
                    logger.info("Not enough liquidity for volume", symbol=symbol, volume=working_vol)
                    continue

                # Пересчитываем спред с учетом РЕАЛЬНЫХ цен
                real_spread_raw = avg_sell_price - avg_buy_price
                real_spread_percent = (real_spread_raw / avg_buy_price) * 100.0
                
                # Вычитаем комиссии (берём из конфига системы)
                buy_fee = 0.0006 # 0.06%
                sell_fee = 0.0006 # 0.06%
                net_real_spread = real_spread_percent - (buy_fee + sell_fee) * 100.0

                logger.info("Real spread calculated", 
                            symbol=symbol, 
                            vol=working_vol, 
                            net_real=f"{net_real_spread:.4f}%", 
                            threshold=f"{user_setting.min_spread_percent:.4f}%")

                if user_setting.filter_spread_enabled:
                    if net_real_spread < user_setting.min_spread_percent:
                        logger.debug("Spread below threshold", symbol=symbol, net_real=net_real_spread)
                        continue

                # --- ПРОВЕРКА ВРЕМЕНИ ЖИЗНИ ---
                if user_setting.filter_lifetime_enabled:
                    key = (symbol, buy_ex, sell_ex, user.user_id)
                    now = time.time()
                    if key not in self.active_spreads:
                        self.active_spreads[key] = now
                        logger.info("Spread detected, waiting for lifetime", symbol=symbol, wait=user_setting.min_lifetime_seconds)
                        continue
                    else:
                        if now - self.active_spreads[key] < user_setting.min_lifetime_seconds:
                            continue
                
                # Добавляем реальные данные в сообщение
                data["real_buy_price"] = avg_buy_price
                data["real_sell_price"] = avg_sell_price
                data["real_net_spread"] = net_real_spread
                data["vol_used"] = working_vol

                await self.send_alert(user.user_id, data)
                if user_setting.filter_lifetime_enabled:
                    del self.active_spreads[(symbol, buy_ex, sell_ex, user.user_id)]

    async def send_alert(self, user_id, data):
        msg = (
            f"🚀 **АРБИТРАЖНАЯ СВЯЗКА** (#{data['symbol']})\n\n"
            f"🟢 **BUY**: {data['buy_on'].upper()}\n"
            f"   Цена: {data['real_buy_price']:.5f} (VWAP)\n\n"
            f"🔴 **SELL**: {data['sell_on'].upper()}\n"
            f"   Цена: {data['real_sell_price']:.5f} (VWAP)\n\n"
            f"📊 Чистый спред: **{data['real_net_spread']:.3f}%**\n"
            f"💰 Объем: ${data['vol_used']:g}\n"
            f"🕒 Время: {time.strftime('%H:%M:%S')}"
        )
        try:
            await self.bot.send_message(user_id, msg, parse_mode="Markdown")
            logger.info("Alert sent to user", user_id=user_id, symbol=data['symbol'])
        except Exception as e:
            logger.error("Failed to send telegram alert", error=str(e))

async def main():
    engine = SignalEngine()
    await engine.run()

if __name__ == "__main__":
    asyncio.run(main())
