import asyncio
import json
import sys
import os
import time
import structlog
import logging
from typing import Any, cast

# Добавляем корень проекта в пути поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from redis.asyncio import Redis
from telegram_bot.config import settings

# Настройка красивого логирования через structlog
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(colors=True)
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger()

# Настройка event loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass


def calculate_vwap(layers: list[list[float]] | list[tuple[float, float]], target_usd: float) -> tuple[float, float]:
    """
    Вычисляет средневзвешенную цену (VWAP) на основе стакана ордеров.
    Полный эквивалент логики функции calculate_vwap из Rust Core.
    """
    if target_usd <= 0.0:
        return 0.0, 0.0

    total_usd = 0.0
    total_amount = 0.0

    for price, amount in layers:
        if price <= 0.0 or amount <= 0.0:
            continue
        layer_usd = price * amount
        if total_usd + layer_usd >= target_usd:
            remaining_usd = target_usd - total_usd
            partially_consumed_amount = remaining_usd / price
            total_amount += partially_consumed_amount
            total_usd = target_usd
            break
        else:
            total_usd += layer_usd
            total_amount += amount

    if total_usd < target_usd or total_amount == 0.0:
        return 0.0, 0.0
    else:
        return target_usd / total_amount, total_usd



def run_startup_tests():
    """
    Автономный Unit-тест с эталонными значениями (asks, bids, volume, vwap_buy, vwap_sell, slippage)
    и проверкой их точного совпадения. Запускается автоматически при старте скрипта.
    """
    logger.info("[TEST] Running startup self-tests with ground-truth values...")
    
    # 1. Задаем эталонные стаканы и параметры
    asks = [(100.0, 2.0), (105.0, 4.0), (110.0, 10.0)]
    bids = [(95.0, 3.0), (90.0, 5.0), (85.0, 10.0)]
    volume = 300.0
    
    # 2. Вычисляем vwap
    vwap_buy, usd_buy = calculate_vwap(asks, volume)
    vwap_sell, usd_sell = calculate_vwap(bids, volume)
    
    # 3. Проверяем результаты vwap
    # vwap_buy точно равен 3150.0 / 31.0 ≈ 101.61290322580645
    # vwap_sell точно равен 1800.0 / 19.0 ≈ 94.73684210526315
    assert abs(vwap_buy - 101.61290322580645) < 1e-9, f"vwap_buy mismatch: {vwap_buy}"
    assert abs(usd_buy - 300.0) < 1e-9
    assert abs(vwap_sell - 94.73684210526315) < 1e-9, f"vwap_sell mismatch: {vwap_sell}"
    assert abs(usd_sell - 300.0) < 1e-9
    
    # 4. Проверяем расчет спредов и проскальзывания
    buy_price = 100.0
    sell_price = 95.0
    buy_fee = 0.0006
    sell_fee = 0.0006
    
    # Лучшие цены (Best)
    spread_raw = sell_price - buy_price
    spread_percent = (spread_raw / buy_price) * 100.0
    net_spread_percent = spread_percent - (buy_fee + sell_fee) * 2.0 * 100.0
    
    # VWAP цены
    eff_spread_percent = ((vwap_sell - vwap_buy) / vwap_buy) * 100.0
    eff_net_spread = eff_spread_percent - (buy_fee + sell_fee) * 2.0 * 100.0
    
    # Проскальзывание (Slippage)
    slippage = net_spread_percent - eff_net_spread
    
    # Проверка эталонных значений
    assert abs(net_spread_percent - (-5.24)) < 1e-9, f"net_spread mismatch: {net_spread_percent}"
    assert abs(eff_net_spread - (-7.006917293233083)) < 1e-9, f"eff_net_spread mismatch: {eff_net_spread}"
    assert abs(slippage - 1.766917293233083) < 1e-9, f"slippage mismatch: {slippage}"
    
    logger.info("[OK] Startup self-tests passed successfully! Python calculations perfectly match Rust math model.")


class MathValidator:
    redis: Redis
    is_running: bool
    total_signals: int
    total_checks: int
    mismatches_vwap_buy: int
    mismatches_vwap_sell: int
    mismatches_slippage: int
    mismatches_net_spread: int
    max_diff_vwap_buy: float
    max_diff_vwap_sell: float
    max_diff_slippage: float
    max_diff_net_spread: float
    reporter_task: "asyncio.Task[None] | None"

    def __init__(self) -> None:
        self.redis = Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
        self.is_running = True
        
        # Метрики
        self.total_signals = 0
        self.total_checks = 0
        self.mismatches_vwap_buy = 0
        self.mismatches_vwap_sell = 0
        self.mismatches_slippage = 0
        self.mismatches_net_spread = 0
        
        # Максимальные отклонения
        self.max_diff_vwap_buy = 0.0
        self.max_diff_vwap_sell = 0.0
        self.max_diff_slippage = 0.0
        self.max_diff_net_spread = 0.0
        self.reporter_task = None

    async def start(self) -> None:
        run_startup_tests()
        
        logger.info("[REDIS] Connecting to Redis stream 'market_signals'...", host=settings.redis_host, port=settings.redis_port)
        stream_name = "market_signals"
        
        # По умолчанию читаем новые сообщения ($)
        # Если хотите проверить исторические, можно заменить на "0-0"
        last_id = "$"
        logger.info("[RUNNING] Math Validator is active and monitoring stream...")

        # Запускаем фоновую задачу периодического отчета по статистике
        self.reporter_task = asyncio.create_task(self.statistics_reporter())

        try:
            while self.is_running:
                response = await self.redis.xread({stream_name: last_id}, count=50, block=2000)
                if not response:
                    continue
                
                typed_response = cast(list[tuple[str, list[tuple[str, dict[str, str]]]]], response)
                for _, messages in typed_response:
                    for msg_id, data in messages:
                        raw_payload = data.get("data")
                        if raw_payload:
                            try:
                                payload = json.loads(raw_payload)
                                if isinstance(payload, dict):
                                    await self.validate_signal(payload)
                            except Exception as pe:
                                logger.error("Failed to parse or validate signal", error=str(pe), msg_id=msg_id)
                        last_id = msg_id
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("Stop requested.")
        except Exception as e:
            logger.error("Error in Stream reader loop", error=str(e))
        finally:
            self.is_running = False
            if self.reporter_task is not None:
                self.reporter_task.cancel()
                try:
                    await self.reporter_task
                except asyncio.CancelledError:
                    pass
            await self.redis.close()
            logger.info("🔌 Redis connection closed.")

    async def validate_signal(self, signal: dict[str, Any]) -> None:
        self.total_signals += 1
        
        symbol = str(signal.get("symbol") or "UNKNOWN")
        buy_on = str(signal.get("buy_on") or "UNKNOWN")
        sell_on = str(signal.get("sell_on") or "UNKNOWN")
        buy_price = float(signal.get("buy_price") or 0.0)
        sell_price = float(signal.get("sell_price") or 0.0)
        buy_fee = float(signal.get("buy_fee") or 0.0)
        sell_fee = float(signal.get("sell_fee") or 0.0)
        rust_net_spread = float(signal.get("net_spread") or 0.0)
        
        buy_book = signal.get("buy_book")
        if not isinstance(buy_book, dict):
            buy_book = {}
        sell_book = signal.get("sell_book")
        if not isinstance(sell_book, dict):
            sell_book = {}
            
        # Валидация консистентности стаканов
        buy_asks_check = buy_book.get("asks")
        if isinstance(buy_asks_check, list) and len(buy_asks_check) > 0:
            first_ask = buy_asks_check[0]
            if isinstance(first_ask, (list, tuple)) and len(first_ask) >= 1:
                try:
                    first_ask_price = float(first_ask[0])
                    if abs(first_ask_price - buy_price) > 1e-6:
                        logger.error(
                            "[ERROR] BOOK CONSISTENCY ERROR: buy_price doesn't match top ask in buy_book!",
                            symbol=symbol,
                            buy_price=buy_price,
                            top_ask=first_ask_price,
                            diff=abs(first_ask_price - buy_price)
                        )
                except (ValueError, TypeError):
                    pass

        sell_bids_check = sell_book.get("bids")
        if isinstance(sell_bids_check, list) and len(sell_bids_check) > 0:
            first_bid = sell_bids_check[0]
            if isinstance(first_bid, (list, tuple)) and len(first_bid) >= 1:
                try:
                    first_bid_price = float(first_bid[0])
                    if abs(first_bid_price - sell_price) > 1e-6:
                        logger.error(
                            "[ERROR] BOOK CONSISTENCY ERROR: sell_price doesn't match top bid in sell_book!",
                            symbol=symbol,
                            sell_price=sell_price,
                            top_bid=first_bid_price,
                            diff=abs(first_bid_price - sell_price)
                        )
                except (ValueError, TypeError):
                    pass
        
        user_metrics = signal.get("user_metrics")
        if not isinstance(user_metrics, dict):
            user_metrics = {}
        
        # Рассчитываем чистый спред (net spread) по лучшим ценам в Python
        py_spread_raw = ((sell_price - buy_price) / buy_price) * 100.0 if buy_price > 0.0 else 0.0
        py_net_spread = py_spread_raw - (buy_fee + sell_fee) * 2.0 * 100.0
        
        # Валидация нет-спреда
        diff_net_spread = abs(py_net_spread - rust_net_spread)
        if diff_net_spread > self.max_diff_net_spread:
            self.max_diff_net_spread = diff_net_spread
            
        net_spread_discrepancy = diff_net_spread > 1e-6
        if net_spread_discrepancy:
            self.mismatches_net_spread += 1
            logger.warning(
                "[WARN] Net Spread mismatch detected!",
                symbol=symbol,
                buy_on=buy_on,
                sell_on=sell_on,
                py_net_spread=py_net_spread,
                rust_net_spread=rust_net_spread,
                diff=diff_net_spread
            )

        # Проходимся по метрикам каждого пользователя (разные объемы)
        for user_id, metrics in user_metrics.items():
            if not isinstance(metrics, dict):
                continue
            self.total_checks += 1
            working_volume = float(metrics.get("working_volume") or 0.0)
            rust_vwap_buy = float(metrics.get("vwap_buy") or 0.0)
            rust_vwap_sell = float(metrics.get("vwap_sell") or 0.0)
            rust_slippage = float(metrics.get("slippage") or 0.0)
            
            # Вычисление в Python по сырым стаканам
            buy_asks = buy_book.get("asks")
            if not isinstance(buy_asks, list):
                buy_asks = []
            sell_bids = sell_book.get("bids")
            if not isinstance(sell_bids, list):
                sell_bids = []

            # Для надежности приводим слои к списку float-кортежей
            typed_asks: list[tuple[float, float]] = []
            for item in buy_asks:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    try:
                        typed_asks.append((float(item[0]), float(item[1])))
                    except (ValueError, TypeError):
                        continue

            typed_bids: list[tuple[float, float]] = []
            for item in sell_bids:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    try:
                        typed_bids.append((float(item[0]), float(item[1])))
                    except (ValueError, TypeError):
                        continue

            py_vwap_buy, _ = calculate_vwap(typed_asks, working_volume)
            py_vwap_sell, _ = calculate_vwap(typed_bids, working_volume)
            
            if py_vwap_buy > 0.0 and py_vwap_sell > 0.0:
                py_eff_spread_percent = ((py_vwap_sell - py_vwap_buy) / py_vwap_buy) * 100.0
                py_eff_net_spread = py_eff_spread_percent - (buy_fee + sell_fee) * 2.0 * 100.0
                py_slippage = py_net_spread - py_eff_net_spread
            else:
                py_slippage = 0.0
                
            # Расчет разниц
            diff_vwap_buy = abs(py_vwap_buy - rust_vwap_buy)
            diff_vwap_sell = abs(py_vwap_sell - rust_vwap_sell)
            diff_slippage = abs(py_slippage - rust_slippage)
            
            # Обновляем максимальные отклонения
            if diff_vwap_buy > self.max_diff_vwap_buy:
                self.max_diff_vwap_buy = diff_vwap_buy
            if diff_vwap_sell > self.max_diff_vwap_sell:
                self.max_diff_vwap_sell = diff_vwap_sell
            if diff_slippage > self.max_diff_slippage:
                self.max_diff_slippage = diff_slippage
                
            # Проверка погрешностей
            has_discrepancy = False
            tolerance = 1e-6
            
            if diff_vwap_buy > tolerance:
                self.mismatches_vwap_buy += 1
                has_discrepancy = True
            if diff_vwap_sell > tolerance:
                self.mismatches_vwap_sell += 1
                has_discrepancy = True
            if diff_slippage > tolerance:
                self.mismatches_slippage += 1
                has_discrepancy = True
                
            if has_discrepancy:
                logger.error(
                    "[ERROR] MATH DISCREPANCY DETECTED!",
                    symbol=symbol,
                    user_id=user_id,
                    volume=working_volume,
                    vwap_buy={"py": py_vwap_buy, "rust": rust_vwap_buy, "diff": diff_vwap_buy},
                    vwap_sell={"py": py_vwap_sell, "rust": rust_vwap_sell, "diff": diff_vwap_sell},
                    slippage={"py": py_slippage, "rust": rust_slippage, "diff": diff_slippage},
                    raw_bids_top=typed_bids[:3],
                    raw_asks_top=typed_asks[:3]
                )
            else:
                logger.info(
                    "[OK] Signal Math Verification Passed",
                    symbol=symbol,
                    buy=f"{buy_on.upper()} -> {sell_on.upper()}",
                    vol=f"${working_volume:.0f}",
                    vwap_buy=f"{rust_vwap_buy:.5f}",
                    vwap_sell=f"{rust_vwap_sell:.5f}",
                    slip=f"{rust_slippage:.4f}%"
                )

    async def statistics_reporter(self) -> None:
        """Периодически выводит в логи агрегированную статистику."""
        while self.is_running:
            await asyncio.sleep(15)
            logger.info(
                "[STATS] VALIDATOR RUNTIME STATS",
                signals_checked=self.total_signals,
                user_calculations_checked=self.total_checks,
                mismatches_net_spread=self.mismatches_net_spread,
                mismatches_vwap_buy=self.mismatches_vwap_buy,
                mismatches_vwap_sell=self.mismatches_vwap_sell,
                mismatches_slippage=self.mismatches_slippage,
                max_diff_net_spread=f"{self.max_diff_net_spread:.10f}",
                max_diff_vwap_buy=f"{self.max_diff_vwap_buy:.10f}",
                max_diff_vwap_sell=f"{self.max_diff_vwap_sell:.10f}",
                max_diff_slippage=f"{self.max_diff_slippage:.10f}",
            )


if __name__ == "__main__":
    validator = MathValidator()
    try:
        asyncio.run(validator.start())
    except KeyboardInterrupt:
        logger.info("👋 Math Validator stopped by user.")

