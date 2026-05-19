import sys
import asyncio
import socket
import json
import ccxt.async_support as ccxt
import structlog
from redis.asyncio import Redis
from sqlalchemy import select
from telegram_bot.config import settings
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import UserSetting
from aiohttp.resolver import ThreadedResolver
import aiohttp

# Фикс DNS и Event Loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = structlog.get_logger()

# Кастомный коннектор для aiohttp, чтобы избежать таймаутов DNS на Windows
def get_custom_session():
    connector = aiohttp.TCPConnector(resolver=ThreadedResolver(), family=socket.AF_INET)
    return aiohttp.ClientSession(connector=connector)

class VolumeMonitor:
    def __init__(self):
        self.redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
        # Инициализируем биржи (только фьючерсы) с кастомной сессией
        self.exchanges = {
            "binance": ccxt.binance({"options": {"defaultType": "future"}}),
            "bybit": ccxt.bybit({"options": {"defaultType": "future"}}),
            "okx": ccxt.okx({"options": {"defaultType": "swap"}})
        }
        
    async def init_sessions(self):
        for ex in self.exchanges.values():
            ex.session = get_custom_session()

    async def fetch_volumes(self, exchange_id, exchange):
        """Получает тикеры и фильтрует USDT фьючерсы по объему."""
        try:
            tickers = await exchange.fetch_tickers()
            valid_symbols = []
            
            # Получаем минимальный порог из БД (берем максимальный среди всех юзеров для безопасности)
            async with AsyncSessionLocal() as session:
                stmt = select(UserSetting.min_volume_24h)
                result = await session.execute(stmt)
                volumes = result.scalars().all()
                min_vol_threshold = min(volumes) if volumes else 1_000_000

            for symbol, data in tickers.items():
                # Фильтруем: только USDT пары и только Futures/Swap
                # CCXT нормализует фьючерсы как 'BTC/USDT:USDT' или 'BTC/USDT'
                quote_vol = data.get('quoteVolume')
                if not quote_vol:
                    # Fallback для OKX (использует volCcy24h в сыром ответе)
                    vol_ccy = data.get('info', {}).get('volCcy24h', 0)
                    try:
                        quote_vol = float(vol_ccy)
                    except (ValueError, TypeError):
                        quote_vol = 0

                if "/USDT" in symbol and quote_vol >= min_vol_threshold:
                    valid_symbols.append(symbol)
            
            return set(valid_symbols)
        except Exception as e:
            logger.error(f"Error fetching volumes from {exchange_id}", error=str(e))
            return set()

    async def update_active_symbols(self):
        """Основной цикл мониторинга объемов и параметров фильтрации."""
        logger.info("Volume Monitor started")
        while True:
            try:
                # 1. Транслируем минимальный спред из БД в Redis для Rust
                async with AsyncSessionLocal() as session:
                    stmt = select(UserSetting.min_spread_percent)
                    result = await session.execute(stmt)
                    spreads = result.scalars().all()
                    if spreads:
                        # Берем минимальный спред среди всех пользователей (самый широкий охват)
                        global_min_spread = min(spreads)
                        await self.redis.set("global_min_spread", str(global_min_spread))
                        logger.info("Global min spread updated in Redis", value=global_min_spread)

                # 2. Собираем данные об объемах
                tasks = [self.fetch_volumes(eid, ex) for eid, ex in self.exchanges.items()]
                results = await asyncio.gather(*tasks)
                
                # 3. Формируем маппинг символов для стратегии 2+ биржи
                exchange_ids = list(self.exchanges.keys())
                symbol_to_exchanges = {}
                for i, liquid_symbols in enumerate(results):
                    ex_id = exchange_ids[i]
                    for symbol in liquid_symbols:
                        # CCXT возвращает тикеры вида BTC/USDT:USDT. Нам нужен чистый BTC/USDT
                        clean_symbol = symbol.split(':')[0]
                        if clean_symbol not in symbol_to_exchanges:
                            symbol_to_exchanges[clean_symbol] = []
                        symbol_to_exchanges[clean_symbol].append(ex_id)
                
                # Оставляем только те символы, которые ликвидны МИНИМУМ на двух биржах
                final_whitelist = {
                    symbol: exchanges 
                    for symbol, exchanges in symbol_to_exchanges.items() 
                    if len(exchanges) >= 2
                }
                
                if final_whitelist:
                    await self.redis.set("active_symbols_map", json.dumps(final_whitelist))
                    logger.info("Whitelist updated", count=len(final_whitelist))
                else:
                    logger.warn("No liquid symbols found on 2+ exchanges")

            except Exception as e:
                logger.error("Error in Volume Monitor loop", error=str(e))
            
            # Спим 1 минуту (критическое требование пользователя)
            await asyncio.sleep(60)

    async def close(self):
        for ex in self.exchanges.values():
            await ex.close()

async def main():
    monitor = VolumeMonitor()
    await monitor.init_sessions()
    try:
        await monitor.update_active_symbols()
    finally:
        await monitor.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
