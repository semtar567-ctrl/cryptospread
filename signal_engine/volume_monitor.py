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
            "mexc": ccxt.mexc({"options": {"defaultType": "swap"}}),
            "bitget": ccxt.bitget({"options": {"defaultType": "swap"}}),
            "gateio": ccxt.gateio({"options": {"defaultType": "swap"}}),
            "bingx": ccxt.bingx({"options": {"defaultType": "swap"}}),
            "kucoinfutures": ccxt.kucoinfutures({"options": {"defaultType": "swap"}}),
            "hyperliquid": None,
            "asterdex": None,
            "bitmart": ccxt.bitmart({"options": {"defaultType": "swap"}}),
            "htx": ccxt.htx({"options": {"defaultType": "swap"}}),
            "dydx": None,
            "lbank": ccxt.lbank({"options": {"defaultType": "swap"}}),
            "xt": ccxt.xt({"options": {"defaultType": "swap"}}),
            "coinexfutures": ccxt.coinex({"options": {"defaultType": "swap"}})
        }
        
    async def init_sessions(self):
        self.session = get_custom_session()
        for ex in self.exchanges.values():
            if ex is not None:
                ex.session = self.session

    async def fetch_exchange_meta(self, exchange_id, exchange):
        """
        Загружает рынки и фильтрует только бессрочные фьючерсы (Swaps).
        Возвращает: { unified_symbol: { "id": exchange_id_symbol, "vol": 24h_vol, "funding": rate } }
        """
        if exchange_id == "hyperliquid":
            return await self.fetch_hyperliquid_meta()
        if exchange_id == "asterdex":
            return await self.fetch_asterdex_meta()
        if exchange_id == "dydx":
            return await self.fetch_dydx_meta()
        try:
            markets = await exchange.load_markets()
            if exchange_id == "coinexfutures":
                tickers = await exchange.fetch_tickers(params={"type": "swap"})
            else:
                tickers = await exchange.fetch_tickers()
            
            # Получаем фандинг (опционально, если биржа поддерживает)
            funding_rates = {}
            next_funding_times = {}
            
            # 1. Сначала пробуем вытащить фандинг из тикеров (самый быстрый и надежный способ для Bybit/MEXC)
            for symbol, ticker in tickers.items():
                try:
                    info = ticker.get("info", {})
                    # Bybit & MEXC & OKX часто кладут fundingRate в info тикера
                    fr = info.get("fundingRate") or info.get("lastFundingRate")
                    if fr is not None:
                        funding_rates[symbol] = float(fr)
                    
                    # Время следующего фандинга
                    nft = info.get("nextFundingTime") or info.get("nextSettleTime")
                    if nft:
                        next_funding_times[symbol] = int(nft)
                except: pass

            # 2. Если данных мало, пробуем специализированный метод (для Binance/OKX)
            if exchange_id in ["binance", "okx"]:
                try:
                    funding_data = await exchange.fetch_funding_rates()
                    for symbol, data in funding_data.items():
                        if symbol not in funding_rates:
                            funding_rates[symbol] = data.get("fundingRate", 0)
                        if symbol not in next_funding_times:
                            next_funding_times[symbol] = data.get("nextFundingTimestamp")
                except:
                    logger.warn(f"Could not fetch bulk funding rates for {exchange_id}")

            results = {}
            for symbol, market in markets.items():
                # СТРОГИЕ ФИЛЬТРЫ:
                # 1. Только Swaps (бессрочные фьючерсы)
                # 2. Только USDT/USDC обеспечение (Linear)
                # 3. Активные торги
                if not market.get("swap"): continue
                if market.get("settle") not in ["USDT", "USDC"]: continue
                if market.get("active") is False: continue

                # 4. Исключаем Pre-Market, Pre-IPO, RWA (акции), TradFi и не-бессрочные контракты
                info = market.get("info", {})
                u_type = str(info.get("underlyingType", "")).upper()
                s_type = str(info.get("symbolType", "")).upper()
                if u_type in ["PREMARKET", "EQUITY", "STOCK"] or s_type in ["STOCK", "STOCKS"] or info.get("isPreMarket") is True or info.get("isRwa") == "YES": continue
                c_type = (str(info.get("contractType", "")) + " " + str(info.get("contract_type", ""))).upper()
                if "TRADIFI" in c_type or "STOCK" in c_type or "QUARTER" in c_type or "DELIVERY" in c_type: continue
                
                # Нормализация символа и вычисление префиксного мультипликатора контракта
                raw_base = symbol.split(":")[0].split("/")[0].upper()
                
                prefix_mult = 1.0
                matched_prefix = None
                known_prefix_memes = {"PEPE", "BONK", "SATS", "MOG", "BABYDOGE", "CHEEMS", "MUU", "CAT", "NEX", "FLOKI", "SHIB"}

                for prefix in ["1000000", "10000", "1000"]:
                    if raw_base.startswith(prefix) and len(raw_base) > len(prefix):
                        prefix_mult = float(prefix)
                        matched_prefix = prefix
                        break

                if matched_prefix:
                    unified = f"{raw_base}USDT"
                elif raw_base in known_prefix_memes:
                    prefix_key = "1000000" if raw_base in ["MOG", "BABYDOGE", "MUU"] else "1000"
                    unified = f"{prefix_key}{raw_base}USDT"
                else:
                    unified = f"{raw_base}USDT"

                total_multiplier = prefix_mult
                
                # Получаем объем из тикера
                ticker = tickers.get(symbol, {})
                quote_vol = ticker.get("quoteVolume") or (float(ticker["info"].get("value", 0)) if "info" in ticker and ticker["info"].get("value") else 0)
                
                # Если объем нулевой, пробуем вытащить из info (специфично для OKX / CoinEx)
                if not quote_vol and "info" in ticker:
                    quote_vol = float(ticker["info"].get("volCcy24h", 0)) or float(ticker["info"].get("value", 0))

                results[unified] = {
                    "exchange_id_symbol": market["id"], # ВАЖНО: Точный ID биржи (напр. BTC-USDT-SWAP)
                    "volume_24h": quote_vol or 0.0,
                    "funding_rate": funding_rates.get(symbol) or 0.0,
                    "next_funding_at": next_funding_times.get(symbol),
                    "last_price": ticker.get("last") or 0.0,
                    "taker_fee": market.get("taker") if market.get("taker") is not None else 0.0006,
                    "multiplier": total_multiplier,
                    "contract_size": float(market.get("contractSize") or 1.0)
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

    async def fetch_hyperliquid_meta(self):
        """
        Загружает мета-данные бессрочных фьючерсов Hyperliquid напрямую из REST API:
        https://api.hyperliquid.xyz/info payload: {"type": "metaAndAssetCtxs"}
        """
        url = "https://api.hyperliquid.xyz/info"
        headers = {"Content-Type": "application/json"}
        payload = {"type": "metaAndAssetCtxs"}

        try:
            session = getattr(self, "session", None) or get_custom_session()
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"Hyperliquid API error status: {resp.status}")
                    return {}
                data = await resp.json()

            if not isinstance(data, list) or len(data) < 2:
                logger.error("Invalid response format from Hyperliquid info endpoint")
                return {}

            universe = data[0].get("universe", [])
            asset_ctxs = data[1]

            known_prefix_memes = {"PEPE", "BONK", "SATS", "MOG", "BABYDOGE", "CHEEMS", "MUU", "CAT", "NEX", "FLOKI", "SHIB"}

            results = {}
            for idx, asset in enumerate(universe):
                coin = asset.get("name")
                if not coin or idx >= len(asset_ctxs):
                    continue

                ctx = asset_ctxs[idx]
                volume_24h = float(ctx.get("dayNtlVlm", 0) or 0)
                funding_rate = float(ctx.get("funding", 0) or 0)
                last_price = float(ctx.get("midPx") or ctx.get("markPx") or ctx.get("oraclePx") or 0)

                raw_coin = coin.upper()
                prefix_mult = 1.0
                matched_prefix = None

                # Обработка префикса 'k' на Hyperliquid (kPEPE -> 1000PEPE)
                if raw_coin.startswith("K") and len(raw_coin) > 1 and raw_coin[1:] in known_prefix_memes:
                    raw_base = f"1000{raw_coin[1:]}"
                    prefix_mult = 1000.0
                    matched_prefix = "1000"
                else:
                    raw_base = raw_coin

                for prefix in ["1000000", "10000", "1000"]:
                    if raw_base.startswith(prefix) and len(raw_base) > len(prefix):
                        prefix_mult = float(prefix)
                        matched_prefix = prefix
                        break

                if matched_prefix:
                    unified = f"{raw_base}USDT"
                elif raw_base in known_prefix_memes:
                    prefix_key = "1000000" if raw_base in ["MOG", "BABYDOGE", "MUU"] else "1000"
                    unified = f"{prefix_key}{raw_base}USDT"
                else:
                    unified = f"{raw_base}USDT"

                results[unified] = {
                    "exchange_id_symbol": coin,
                    "volume_24h": volume_24h,
                    "funding_rate": funding_rate,
                    "next_funding_at": None,
                    "last_price": last_price,
                    "taker_fee": 0.00035,
                    "multiplier": prefix_mult,
                    "contract_size": 1.0
                }

            logger.info(f"Fetched {len(results)} swaps from hyperliquid")
            return results

        except Exception as e:
            logger.error("Error fetching meta from hyperliquid", error=str(e))
            return {}

    async def fetch_asterdex_meta(self):
        """
        Загружает мета-данные бессрочных фьючерсов Aster DEX напрямую из REST API:
        https://fapi.asterdex.com/fapi/v1/ticker/24hr
        https://fapi.asterdex.com/fapi/v1/premiumIndex
        """
        ticker_url = "https://fapi.asterdex.com/fapi/v1/ticker/24hr"
        funding_url = "https://fapi.asterdex.com/fapi/v1/premiumIndex"

        try:
            session = getattr(self, "session", None) or get_custom_session()
            async with session.get(ticker_url, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"Aster DEX ticker API error status: {resp.status}")
                    return {}
                tickers_data = await resp.json()

                funding_map = {}
                next_funding_map = {}
                async with session.get(funding_url, timeout=10) as resp:
                    if resp.status == 200:
                        funding_data = await resp.json()
                        if isinstance(funding_data, list):
                            for item in funding_data:
                                sym = item.get("symbol")
                                if sym:
                                    fr = item.get("lastFundingRate")
                                    nft = item.get("nextFundingTime")
                                    if fr is not None:
                                        funding_map[sym] = float(fr)
                                    if nft:
                                        next_funding_map[sym] = int(nft)
                        elif isinstance(funding_data, dict):
                            sym = funding_data.get("symbol")
                            if sym:
                                fr = funding_data.get("lastFundingRate")
                                nft = funding_data.get("nextFundingTime")
                                if fr is not None:
                                    funding_map[sym] = float(fr)
                                if nft:
                                    next_funding_map[sym] = int(nft)

            if not isinstance(tickers_data, list):
                if isinstance(tickers_data, dict):
                    tickers_data = [tickers_data]
                else:
                    logger.error("Invalid response format from Aster DEX ticker endpoint")
                    return {}

            known_prefix_memes = {"PEPE", "BONK", "SATS", "MOG", "BABYDOGE", "CHEEMS", "MUU", "CAT", "NEX", "FLOKI", "SHIB"}

            results = {}
            for item in tickers_data:
                symbol = item.get("symbol")
                if not symbol or not symbol.endswith("USDT"):
                    continue

                quote_vol = float(item.get("quoteVolume", 0) or 0)
                last_price = float(item.get("lastPrice") or item.get("last_price") or 0)

                raw_base = symbol[:-4].upper()
                prefix_mult = 1.0
                matched_prefix = None

                for prefix in ["1000000", "10000", "1000"]:
                    if raw_base.startswith(prefix) and len(raw_base) > len(prefix):
                        prefix_mult = float(prefix)
                        matched_prefix = prefix
                        break

                if matched_prefix:
                    unified = f"{raw_base}USDT"
                elif raw_base in known_prefix_memes:
                    prefix_key = "1000000" if raw_base in ["MOG", "BABYDOGE", "MUU"] else "1000"
                    unified = f"{prefix_key}{raw_base}USDT"
                else:
                    unified = f"{raw_base}USDT"

                results[unified] = {
                    "exchange_id_symbol": symbol,
                    "volume_24h": quote_vol,
                    "funding_rate": funding_map.get(symbol, 0.0),
                    "next_funding_at": next_funding_map.get(symbol),
                    "last_price": last_price,
                    "taker_fee": 0.0004,
                    "multiplier": prefix_mult,
                    "contract_size": 1.0
                }

            logger.info(f"Fetched {len(results)} swaps from asterdex")
            return results

        except Exception as e:
            logger.error("Error fetching meta from asterdex", error=str(e))
            return {}

    async def fetch_dydx_meta(self):
        """
        Загружает мета-данные бессрочных фьючерсов dYdX v4 DEX напрямую из REST API:
        https://indexer.dydx.trade/v4/perpetualMarkets
        """
        url = "https://indexer.dydx.trade/v4/perpetualMarkets"
        try:
            session = getattr(self, "session", None) or get_custom_session()
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"dYdX REST error: HTTP {resp.status}")
                    return {}
                data = await resp.json()
                markets = data.get("markets", {})
                results = {}
                for ticker, m in markets.items():
                    if not ticker.endswith("-USD"):
                        continue
                    base = ticker[:-4].upper()
                    unified = f"{base}USDT"
                    price = float(m.get("oraclePrice") or m.get("indexPrice") or 0.0)
                    vol = float(m.get("volume24H") or 0.0)
                    results[unified] = {
                        "exchange_id_symbol": ticker,
                        "volume_24h": vol,
                        "funding_rate": 0.0,
                        "next_funding_at": None,
                        "last_price": price,
                        "taker_fee": 0.0005,
                        "multiplier": 1.0,
                        "contract_size": 1.0
                    }
                logger.info(f"Fetched {len(results)} swaps from dydx")
                return results
        except Exception as e:
            logger.error("Error fetching meta from dydx", error=str(e))
            return {}

    async def update_all(self):
        logger.info("Meta Monitor (Strict Mode) started")
        while True:
            try:
                # 1. Обновляем глобальный порог спреда
                async with AsyncSessionLocal() as session:
                    stmt = select(UserSetting.min_spread_percent, UserSetting.min_volume_24h, UserSetting.filter_volume_enabled)
                    res = await session.execute(stmt)
                    settings_list = res.all()
                    min_spread = min([s[0] for s in settings_list]) if settings_list else 0.1
                    enabled_vols = [s[1] for s in settings_list if s[2]]
                    min_vol_threshold = min(enabled_vols) if enabled_vols else 0.0
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
                    # Защита от коллизий тикеров: очистка бирж-выбросов по медианной цене
                    valid_prices = [full_map[unified][ex_id]["last_price"] for ex_id in exs if (full_map[unified][ex_id].get("last_price") or 0) > 0]
                    if len(valid_prices) >= 2:
                        sorted_p = sorted(valid_prices)
                        median_p = sorted_p[len(sorted_p) // 2]
                        if median_p > 0:
                            filtered_exs = [ex_id for ex_id in exs if (full_map[unified][ex_id].get("last_price") or 0) > 0 and 0.25 <= (full_map[unified][ex_id]["last_price"] / median_p) <= 4.0]
                            if len(filtered_exs) >= 2:
                                exs = filtered_exs

                    if len(exs) >= 2:
                        # Собираем точные ID и комиссии для каждой биржи
                        ex_mappings = {}
                        for ex_id in exs:
                            ex_mappings[ex_id] = {
                                "id": full_map[unified][ex_id]["exchange_id_symbol"],
                                "fee": full_map[unified][ex_id]["taker_fee"],
                                "multiplier": full_map[unified][ex_id].get("multiplier", 1.0)
                            }
                            
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
            if ex is not None:
                await ex.close()
        if hasattr(self, "session") and self.session and not self.session.closed:
            await self.session.close()

async def main():
    monitor = MetaMonitor()
    await monitor.init_sessions()
    try:
        await monitor.update_all()
    finally:
        await monitor.close()

if __name__ == "__main__":
    asyncio.run(main())
