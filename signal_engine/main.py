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
from telegram_bot.database.models import User
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
                        parse_mode="Markdown", 
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
        # ... (начало метода без изменений)
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

            # 1. Анти-спам (Deduplication) - 60 секунд на одну и ту же связку для юзера
            key = (symbol, buy_ex, sell_ex, uid)
            if key in self.last_alert_sent:
                if now - self.last_alert_sent[key] < 60:
                    continue

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
                    # Новое поле: время следующего фандинга
                    next_f_buy = bm.get("next_funding_at")
                except: pass
            
            if s_meta_raw:
                try:
                    sm = json.loads(s_meta_raw)
                    funding_sell = sm.get("funding_rate", 0.0)
                    vol_24h_sell = sm.get("volume_24h", 0.0)
                    # Новое поле: время следующего фандинга
                    next_f_sell = sm.get("next_funding_at")
                except: pass

            # 3. Подготовка данных
            discovery_ts = data.get("timestamp", int(now * 1000))
            latency_ms = int(time.time() * 1000) - discovery_ts

            vwap_buy = metrics.get("vwap_buy", 0.0)
            vwap_sell = metrics.get("vwap_sell", 0.0)
            slippage = metrics.get("slippage", 0.0)
            net_spread = data["net_spread"]
            eff_net_spread = net_spread - slippage

            signal_info = {
                "symbol": symbol.replace("/", "").replace("-", ""), 
                "buy_ex": buy_ex, 
                "sell_ex": sell_ex,
                "net_spread": net_spread,
                "eff_net_spread": eff_net_spread,
                # Фандинг спред: положительный - это доп. профит для нас
                "funding_spread": (funding_sell - funding_buy) * 100.0,
                "vol_usd": metrics.get("working_volume", 0),
                "buy_price": vwap_buy if vwap_buy > 0 else data["buy_price"],
                "buy_funding": funding_buy * 100, 
                "buy_vol_24h": vol_24h_buy,
                "buy_slip": slippage / 2,
                "buy_next_f": next_f_buy,
                "buy_full": vwap_buy > 0,
                "sell_price": vwap_sell if vwap_sell > 0 else data["sell_price"],
                "sell_funding": funding_sell * 100, 
                "sell_vol_24h": vol_24h_sell,
                "sell_slip": slippage / 2,
                "sell_next_f": next_f_sell,
                "sell_full": vwap_sell > 0,
                "latency": latency_ms
            }

            # Вместо прямой отправки добавляем в очередь
            await self.queue_styled_alert(uid, signal_info)
            self.last_alert_sent[key] = now

    async def queue_styled_alert(self, user_id, s):
        """Формирует сообщение и кладет его в очередь на отправку."""
        tv_symbol = f"BYBIT:{s['symbol']}.P/OKX:{s['symbol']}.P"
        tv_link = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
        
        warn_b = " ⚠️ Мало ликвидности" if not s["buy_full"] else ""
        warn_s = " ⚠️ Мало ликвидности" if not s["sell_full"] else ""

        msg = (
            f"🟦🟧 **{s['symbol']}** (F/F)\n"
            f"↑ {s['buy_ex'].upper()} F. / ↓ {s['sell_ex'].upper()} F.\n\n"
            f"📊 Чистый спред (Best): **{s['net_spread']:.2f}%**\n"
            f"📈 Эфф. спред (VWAP): **{s['eff_net_spread']:.2f}%**\n"
            f"💎 Фандинг Спред: {s['funding_spread']:.3f}%\n"
            f"💰 Твой объем: ${s['vol_usd']:g}\n\n"
            f"🟩 ЛОНГ НА ФЬЮЧЕРСЕ:\n"
            f"[{s['buy_ex'].upper()}]({self._get_ex_link(s['buy_ex'], s['symbol'])})\n"
            f"VWAP Цена: {s['buy_price']:.5f}\n"
            f"Фандинг: {s['buy_funding']:.3f}% | {self._format_time_to_funding(s['buy_next_f'])}\n"
            f"Объем 24ч: ${s['buy_vol_24h']/1e6:.1f}M\n"
            f"Проскальзывание: {s['buy_slip']:.3f}%{warn_b}\n\n"
            f"🟥 ШОРТ НА ФЬЮЧЕРСЕ:\n"
            f"[{s['sell_ex'].upper()}]({self._get_ex_link(s['sell_ex'], s['symbol'])})\n"
            f"VWAP Цена: {s['sell_price']:.5f}\n"
            f"Фандинг: {s['sell_funding']:.3f}% | {self._format_time_to_funding(s['sell_next_f'])}\n"
            f"Объем 24ч: ${s['sell_vol_24h']/1e6:.1f}M\n"
            f"Проскальзывание: {s['sell_slip']:.3f}%{warn_s}\n\n"
            f"⏱ Задержка: {s['latency']}мс\n"
            f"📈 [График спреда]({tv_link})\n"
        )
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🎯 Отслеживать схождение", callback_data=f"track_{s['symbol']}_{s['buy_ex']}_{s['sell_ex']}")]
        ])
        
        await self.msg_queue.put((user_id, msg, kb))
        logger.debug("Signal added to queue", symbol=s['symbol'], qsize=self.msg_queue.qsize())

    def _get_ex_link(self, exchange, symbol):
        return {
            "binance": f"https://www.binance.com/en/futures/{symbol}",
            "bybit": f"https://www.bybit.com/trade/usdt/{symbol}",
            "okx": f"https://www.okx.com/trade-swap/{symbol.lower()}-usdt",
            "mexc": f"https://contract.mexc.com/exchange/{symbol}_USDT"
        }.get(exchange, "#")

    def _format_time_to_funding(self, ts_ms):
        if not ts_ms: return "н/д"
        try:
            d = (int(ts_ms) / 1000) - time.time()
            if d < 0: return "сейчас"
            return f"{int(d // 3600)}ч {int((d % 3600) // 60)}м"
        except: return "н/д"

async def main():
    await SignalEngine().run()

if __name__ == "__main__":
    asyncio.run(main())
