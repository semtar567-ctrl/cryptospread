import sys
import os
import asyncio
import json
import time
import socket
import structlog

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from telegram_bot.config import settings
from telegram_bot.database.engine import AsyncSessionLocal
from telegram_bot.database.models import User, UserSetting, TrackedSpread
from telegram_bot.utils.keyboards import format_signal_card_msg_and_kb
from aiogram import Bot, types
from aiogram.client.session.aiohttp import AiohttpSession
from aiohttp.resolver import ThreadedResolver

# Фикс DNS и Event Loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = structlog.get_logger()

class IPv4Session(AiohttpSession):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._connector_init["resolver"] = ThreadedResolver()
        self._connector_init["family"] = socket.AF_INET

class SignalEngine:
    def __init__(self):
        # Используем настройки из конфига
        self.redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
        self.bot = Bot(token=settings.telegram_bot_token, session=IPv4Session())
        self.active_spreads = {} # (symbol, buy, sell, user_id) -> start_time
        self.last_alert_sent = {} # (symbol, buy, sell, user_id) -> last_time
        self.msg_queue = asyncio.Queue()
        self.is_running = True

    async def run(self):
        logger.info("Signal Engine (Light) started. Waiting for pre-filtered signals from Rust...")
        
        # Запускаем воркер для очереди сообщений (Throttling)
        asyncio.create_task(self.message_worker())
        
        # Стрим, в который пишет Rust
        stream_name = "market_signals"
        
        last_id = "$"
        while self.is_running:
            try:
                response = await self.redis.xread({stream_name: last_id}, count=50, block=1000)
                if not response: continue
                
                for s_name, messages in response:
                    for msg_id, data in messages:
                        raw_payload = data.get("data")
                        if raw_payload:
                            try:
                                await self.process_rust_signal(json.loads(raw_payload))
                            except Exception as pe:
                                logger.error("Parse error", error=str(pe), payload=raw_payload[:100])
                        last_id = msg_id
            except Exception as e:
                logger.error("Error in Signal Engine loop", error=str(e))
                await asyncio.sleep(1)

    async def message_worker(self):
        """Воркер, который отправляет сообщения из очереди с задержкой 1.1 сек для обхода Flood Control."""
        logger.info("Message worker started (Throttling: 1.1s, Queue Limit: 20)")
        while self.is_running:
            try:
                # ОГРАНИЧЕНИЕ ОЧЕРЕДИ: Если сигналов больше 20, удаляем лишние старые
                current_size = self.msg_queue.qsize()
                if current_size > 20:
                    logger.warning("Queue overflow, dropping old signals", current_size=current_size, keep=20)
                    while self.msg_queue.qsize() > 20:
                        try:
                            self.msg_queue.get_nowait()
                            self.msg_queue.task_done()
                        except asyncio.QueueEmpty:
                            break

                user_id, msg, kb = await self.msg_queue.get()

                try:
                    await self.bot.send_message(
                        user_id, 
                        msg, 
                        parse_mode="HTML", 
                        reply_markup=kb, 
                        disable_web_page_preview=True
                    )
                    logger.info("Signal sent to Telegram", user_id=user_id)
                except Exception as e:
                    if "Flood control exceeded" in str(e):
                        logger.error("Flood control still active, waiting longer...", error=str(e))
                        await asyncio.sleep(30) # Если все равно поймали флуд, ждем 30 сек
                    else:
                        logger.error("Failed to send alert in worker", error=str(e))
                
                await asyncio.sleep(1.1) # Задержка между сообщениями
                self.msg_queue.task_done()
            except Exception as e:
                logger.error("Error in message worker", error=str(e))
                await asyncio.sleep(1)

    async def process_rust_signal(self, data):
        symbol = data["symbol"]
        buy_ex = data["buy_on"].lower()
        sell_ex = data["sell_on"].lower()
        target_users = data.get("target_users", [])
        user_metrics = data.get("user_metrics", {})

        if not target_users:
            return

        now = time.time()

        for uid_str in target_users:
            uid = int(uid_str)
            metrics = user_metrics.get(str(uid))
            if not metrics: continue

            # 2. Мета-данные
            meta_key_buy = f"meta:{buy_ex}:{symbol.replace('/', '').replace('-', '')}"
            meta_key_sell = f"meta:{sell_ex}:{symbol.replace('/', '').replace('-', '')}"
            
            b_meta_raw = await self.redis.get(meta_key_buy)
            s_meta_raw = await self.redis.get(meta_key_sell)
            
            funding_buy = 0.0
            funding_sell = 0.0
            vol_24h_buy = 0.0
            vol_24h_sell = 0.0
            next_f_buy = None
            next_f_sell = None

            if b_meta_raw:
                try:
                    bm = json.loads(b_meta_raw)
                    funding_buy = bm.get("funding_rate", 0.0)
                    vol_24h_buy = bm.get("volume_24h", 0.0)
                    next_f_buy = bm.get("next_funding_at")
                except: pass
            
            if s_meta_raw:
                try:
                    sm = json.loads(s_meta_raw)
                    funding_sell = sm.get("funding_rate", 0.0)
                    vol_24h_sell = sm.get("volume_24h", 0.0)
                    next_f_sell = sm.get("next_funding_at")
                except: pass

            # 3. Подготовка данных
            discovery_ts = data.get("timestamp", int(now * 1000))
            latency_ms = int(time.time() * 1000) - discovery_ts

            vwap_buy = metrics.get("vwap_buy", 0.0)
            vwap_sell = metrics.get("vwap_sell", 0.0)
            slippage = metrics.get("slippage", 0.0)
            
            buy_price = data["buy_price"]
            sell_price = data["sell_price"]
            b_fee = data.get("buy_fee", 0.0006)
            s_fee = data.get("sell_fee", 0.0006)
            
            total_fee_percent = (b_fee + s_fee) * 2.0 * 100.0
            raw_spread_percent = ((sell_price - buy_price) / buy_price) * 100.0
            net_spread_after_fees = raw_spread_percent - total_fee_percent
            vwap_net_spread = net_spread_after_fees - slippage

            signal_info = {
                "symbol": symbol.replace("/", "").replace("-", ""), 
                "buy_ex": buy_ex, 
                "sell_ex": sell_ex,
                "total_fee": total_fee_percent,
                "net_spread": net_spread_after_fees,
                "funding_spread": (funding_sell - funding_buy) * 100.0,
                "vol_usd": metrics.get("working_volume", 0),
                "vwap_net_spread": vwap_net_spread,
                "buy_price": vwap_buy if vwap_buy > 0 else buy_price,
                "buy_funding": funding_buy * 100, 
                "buy_vol_24h": vol_24h_buy,
                "buy_slip": slippage / 2.0,
                "buy_next_f": next_f_buy,
                "buy_full": vwap_buy > 0,
                "sell_price": vwap_sell if vwap_sell > 0 else sell_price,
                "sell_funding": funding_sell * 100, 
                "sell_vol_24h": vol_24h_sell,
                "sell_slip": slippage / 2.0,
                "sell_next_f": next_f_sell,
                "sell_full": vwap_sell > 0,
                "latency": latency_ms
            }

            # Записываем спред в Redis для меню "Связки" с TTL 15 секунд
            spread_key = f"current_spread:futures_futures:{symbol.replace('/', '').replace('-', '')}:{buy_ex}:{sell_ex}"
            await self.redis.set(spread_key, json.dumps(signal_info), ex=15)

            # Проверяем активные отслеживаемые спреды (схождение)
            async with AsyncSessionLocal() as session:
                stmt = select(TrackedSpread).where(
                    TrackedSpread.user_id == uid,
                    TrackedSpread.symbol == symbol.replace('/', '').replace('-', ''),
                    TrackedSpread.buy_exchange == buy_ex,
                    TrackedSpread.sell_exchange == sell_ex,
                    TrackedSpread.is_active == True
                )
                result = await session.execute(stmt)
                tracked_records = result.scalars().all()
                
                for record in tracked_records:
                    if vwap_net_spread <= record.target_convergence:
                        alert_msg = (
                            f"🎯 **СХОЖДЕНИЕ СПРЕДА!**\n\n"
                            f"🟦🟧 **{record.symbol}** (F/F)\n"
                            f"↑ {record.buy_exchange.upper()} F. / ↓ {record.sell_exchange.upper()} F.\n\n"
                            f"📈 **Спред сошелся до: {vwap_net_spread:.2f}%**\n"
                            f"🎯 Цель схождения: {record.target_convergence:.2f}%\n"
                            f"💰 Рабочий объем: ${metrics.get('working_volume', 0):g}\n\n"
                            f"✅ **Связка отработала. Позицию можно закрывать!**"
                        )
                        await self.msg_queue.put((uid, alert_msg, None))
                        
                        record.is_active = False
                        await session.commit()
                        logger.info("Spread converged! Sent alert", symbol=record.symbol, user_id=uid)
                        
                        await self.redis.publish("user_updates", str(uid))

            # Загружаем настройки пользователя из БД, чтобы проверить, нужно ли слать обычный алерт
            async with AsyncSessionLocal() as session:
                settings_stmt = select(UserSetting).where(UserSetting.user_id == uid, UserSetting.trade_type == "futures_futures")
                settings_res = await session.execute(settings_stmt)
                user_settings = settings_res.scalar_one_or_none()
                
                min_spread = user_settings.min_spread_percent if user_settings else 0.1
                filter_spread_enabled = user_settings.filter_spread_enabled if user_settings else True

            is_above_threshold = True
            if filter_spread_enabled:
                if vwap_net_spread < min_spread:
                    is_above_threshold = False

            if is_above_threshold:
                # 1. Анти-спам (60 секунд на монету для каждого юзера)
                sym_clean = symbol.replace("/", "").replace("-", "")
                cooldown_key = (uid, sym_clean)
                if cooldown_key in self.last_alert_sent:
                    if now - self.last_alert_sent[cooldown_key] < 60:
                        continue

                # Группируем все доступные связки по данному тикеру
                spread_keys = await self.redis.keys(f"current_spread:futures_futures:{sym_clean}:*")
                pairs_map = {}
                for skey in spread_keys:
                    sdata = await self.redis.get(skey)
                    if sdata:
                        try:
                            sp = json.loads(sdata)
                            pkey = (sp["buy_ex"], sp["sell_ex"])
                            pairs_map[pkey] = sp
                        except: pass

                pairs_map[(buy_ex, sell_ex)] = signal_info
                aggregated_pairs = list(pairs_map.values())
                aggregated_pairs.sort(key=lambda p: p.get("vwap_net_spread", 0.0), reverse=True)

                # Сохраняем агрегированный набор в Redis для интерактивного переключения в боте (TTL 300с)
                symbol_signal_key = f"symbol_signal:{uid}:{sym_clean}"
                aggregated_payload = {
                    "symbol": sym_clean,
                    "pairs": aggregated_pairs,
                    "updated_at": now
                }
                await self.redis.set(symbol_signal_key, json.dumps(aggregated_payload), ex=300)

                await self.queue_styled_alert(uid, sym_clean, aggregated_pairs, idx=0)
                self.last_alert_sent[cooldown_key] = now

    async def queue_styled_alert(self, user_id, symbol, pairs, idx=0):
        """Формирует карточку сообщения и кладет её в очередь на отправку."""
        if not pairs:
            return
        msg, kb = format_signal_card_msg_and_kb(symbol, pairs, idx)
        await self.msg_queue.put((user_id, msg, kb))
        logger.debug("Signal added to queue", symbol=symbol, qsize=self.msg_queue.qsize())

    def _get_ex_link(self, exchange, symbol):
        return {
            "binance": f"https://www.binance.com/en/futures/{symbol}",
            "bybit": f"https://www.bybit.com/trade/usdt/{symbol}",
            "okx": f"https://www.okx.com/trade-swap/{symbol.lower()}-usdt",
            "mexc": f"https://contract.mexc.com/exchange/{symbol}_USDT"
        }.get(exchange, "#")

    def _format_time_to_funding(self, ts_ms):
        if not ts_ms:
            # Если время не пришло (напр. MEXC), рассчитываем ближайшее 00, 08, 16 UTC
            from datetime import datetime, timedelta, timezone
            now_utc = datetime.now(timezone.utc)
            # Ближайший час кратный 8
            target_hour = (now_utc.hour // 8 + 1) * 8
            if target_hour >= 24:
                target_dt = now_utc.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            else:
                target_dt = now_utc.replace(hour=target_hour, minute=0, second=0, microsecond=0)
            ts_sec = target_dt.timestamp()
        else:
            ts_sec = int(ts_ms) / 1000

        try:
            diff = ts_sec - time.time()
            # Конвертируем в MSK (UTC+3)
            from datetime import datetime, timedelta, timezone
            dt_msk = datetime.fromtimestamp(ts_sec, timezone(timedelta(hours=3)))
            time_str = dt_msk.strftime("%H:%M")
            
            if diff < 0: 
                return f"{time_str} (сейчас)"
            
            hours = int(diff // 3600)
            minutes = int((diff % 3600) // 60)
            return f"{time_str} (через {hours}ч {minutes}м)"
        except: 
            return "н/д"

async def main():
    await SignalEngine().run()

if __name__ == "__main__":
    asyncio.run(main())
