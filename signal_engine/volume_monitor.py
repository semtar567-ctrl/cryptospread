import sys
import os
import asyncio
import socket
import json
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ccxt.async_support as ccxt
import structlog
from redis.asyncio import Redis
from sqlalchemy import select
from telegram_bot.config import settings
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import UserSetting
from aiohttp.resolver import ThreadedResolver
import aiohttp

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = structlog.get_logger()

def get_custom_session():
    connector = aiohttp.TCPConnector(resolver=ThreadedResolver(), family=socket.AF_INET)
    return aiohttp.ClientSession(connector=connector)

class MetaMonitor:
    def __init__(self):
        self.redis = Redis(host="127.0.0.1", port=6380, decode_responses=True)
        # Инициализируем биржи с флагами для фьючерсов
        self.exchanges = {
            "binance": ccxt.binance({"options": {"defaultType": "future"}}),
            "bybit": ccxt.bybit({"options": {"defaultType": "future"}}),
            "okx": ccxt.okx({"options": {"defaultType": "swap"}}),
            "mexc": ccxt.mexc({"options": {"defaultType": "swap"}})
        }
        
    async def init_sessions(self):
        for ex in self.exchanges.values():
            ex.session = get_custom_session()

    async def fetch_exchange_meta(self, exchange_id, exchange):
        """
        Загружает рынки и фильтрует только бессрочные фьючерсы (Swaps).
        Возвращает: { unified_symbol: { "id": exchange_id_symbol, "vol": 24h_vol, "funding": rate } }
        """
        try:
            markets = await exchange.load_markets()
            tickers = await exchange.fetch_tickers()
            
            # Получаем фандинг (опционально, если биржа поддерживает)
            funding_rates = {}
            next_funding_times = {}
            try:
                funding_data = await exchange.fetch_funding_rates()
                for symbol, data in funding_data.items():
                    funding_rates[symbol] = data.get("fundingRate", 0)
                    next_funding_times[symbol] = data.get("nextFundingTimestamp")
            except:
                logger.warn(f"Could not fetch funding rates for {exchange_id}")

            results = {}
            for symbol, market in markets.items():
                # СТРОГИЕ ФИЛЬТРЫ:
                # 1. Только Swaps (бессрочные фьючерсы)
                # 2. Только USDT/USDC обеспечение (Linear)
                # 3. Активные торги
                if not market.get("swap"): continue
                if market.get("settle") not in ["USDT", "USDC"]: continue
                if not market.get("active"): continue
                
                # Приводим к единому формату BTCUSDT
                unified = symbol.split(":")[0].replace("/", "")
                
                # Получаем объем из тикера
                ticker = tickers.get(symbol, {})
                quote_vol = ticker.get("quoteVolume", 0)
                
                # Если объем нулевой, пробуем вытащить из info (специфично для OKX)
                if not quote_vol and "info" in ticker:
                    quote_vol = float(ticker["info"].get("volCcy24h", 0))

                results[unified] = {
                    "exchange_id_symbol": market["id"], # ВАЖНО: Точный ID биржи (напр. BTC-USDT-SWAP)
                    "volume_24h": quote_vol,
                    "funding_rate": funding_rates.get(symbol, 0),
                    "next_funding_at": next_funding_times.get(symbol),
                    "last_price": ticker.get("last", 0)
                }
                
                # Обновляем мета-данные в Redis немедленно для каждого инструмента
                meta_data = results[unified]
                # Мы не можем писать здесь meta_key, так как нам нужен ex_id. 
                # Логика записи перенесена ниже в цикл update_all.
            
            logger.info(f"Fetched {len(results)} swaps from {exchange_id}")
            return results
        except Exception as e:
            logger.error(f"Error fetching meta from {exchange_id}", error=str(e))
            return {}

    async def update_all(self):
        logger.info("Meta Monitor (Strict Mode) started")
        while True:
            try:
                # 1. Обновляем глобальный порог спреда
                async with AsyncSessionLocal() as session:
                    stmt = select(UserSetting.min_spread_percent, UserSetting.min_volume_24h)
                    res = await session.execute(stmt)
                    settings_list = res.all()
                    min_spread = min([s[0] for s in settings_list]) if settings_list else 0.1
                    min_vol_threshold = min([s[1] for s in settings_list]) if settings_list else 1_000_000
                    await self.redis.set("global_min_spread", str(min_spread))

                # 2. Собираем данные параллельно
                tasks = [self.fetch_exchange_meta(eid, ex) for eid, ex in self.exchanges.items()]
                exchange_results = await asyncio.gather(*tasks)
                
                exchange_ids = list(self.exchanges.keys())
                symbol_to_exchanges = {} # unified -> [ex_ids]
                full_map = {} # unified -> ex_id -> meta_data

                for i, meta in enumerate(exchange_results):
                    ex_id = exchange_ids[i]
                    for unified, data in meta.items():
                        # Фильтр по объему
                        if data["volume_24h"] < min_vol_threshold:
                            continue
                            
                        if unified not in symbol_to_exchanges:
                            symbol_to_exchanges[unified] = []
                            full_map[unified] = {}
                        
                        symbol_to_exchanges[unified].append(ex_id)
                        full_map[unified][ex_id] = data

                # 3. Формируем финальный Whitelist (только те, что на 2+ биржах)
                final_whitelist = {}
                for unified, exs in symbol_to_exchanges.items():
                    if len(exs) >= 2:
                        # Собираем точные ID для каждой биржи
                        ex_mappings = {}
                        for ex_id in exs:
                            ex_mappings[ex_id] = full_map[unified][ex_id]["exchange_id_symbol"]
                            
                            # Сохраняем мета-данные (фандинг и т.д.)
                            meta_key = f"meta:{ex_id}:{unified}"
                            await self.redis.set(meta_key, json.dumps(full_map[unified][ex_id]), ex=600)
                        
                        final_whitelist[unified] = ex_mappings

                if final_whitelist:
                    # Записываем маппинг символов: BTCUSDT -> { "binance": "BTCUSDT", "okx": "BTC-USDT-SWAP" }
                    await self.redis.set("active_symbols_map", json.dumps(final_whitelist))
                    logger.info("Whitelist updated (Strict Swaps only)", count=len(final_whitelist))
                else:
                    logger.warn("No liquid symbols found after strict filtering")

            except Exception as e:
                logger.error("Error in Meta Monitor loop", error=str(e))
            
            await asyncio.sleep(60)

    async def close(self):
        for ex in self.exchanges.values():
            await ex.close()

async def main():
    monitor = MetaMonitor()
    await monitor.init_sessions()
    try:
        await monitor.update_all()
    finally:
        await monitor.close()

if __name__ == "__main__":
    asyncio.run(main())
