import pytest
from signal_engine.main import SignalEngine

def test_calculate_vwap_simple():
    engine = SignalEngine()
    # Тест 1: Идеальный случай - объем на одном уровне
    # [[цена, количество], ...]
    levels = [[100.0, 10.0]] # 100 * 10 = $1000
    avg_price = engine.calculate_vwap(levels, 500.0)
    assert avg_price == 100.0

def test_calculate_vwap_multi_level():
    engine = SignalEngine()
    # Тест 2: Объем размазан по двум уровням
    # Уровень 1: 100 * 5 = $500
    # Уровень 2: 110 * 5 = $550
    # Цель: $1000
    levels = [[100.0, 5.0], [110.0, 5.0]]
    # Должны забрать весь 1-й уровень и почти весь 2-й
    # Общий объем Qty = 5 + (500/110) = 5 + 4.5454... = 9.5454...
    # VWAP = 1000 / 9.5454...
    avg_price = engine.calculate_vwap(levels, 1000.0)
    expected_qty = 5.0 + (500.0 / 110.0)
    assert avg_price == pytest.approx(1000.0 / expected_qty)

def test_calculate_vwap_insufficient_liquidity():
    engine = SignalEngine()
    # Тест 3: Ликвидности не хватает
    levels = [[100.0, 1.0]] # Всего $100
    avg_price = engine.calculate_vwap(levels, 1000.0)
    assert avg_price is None

def test_calculate_vwap_real_prices():
    engine = SignalEngine()
    # Тест 4: Реальные цифры (например SOL)
    # Покупаем на $5000
    levels = [
        [150.0, 10.0], # $1500
        [151.0, 20.0], # $3020
        [152.0, 100.0] # Остаток тут
    ]
    # Набрали: 1500 + 3020 = 4520. Нужно еще 480$ с 3-го уровня.
    # Qty с 3-го уровня = 480 / 152 = 3.15789...
    # Total Qty = 10 + 20 + 3.15789... = 33.15789...
    # VWAP = 5000 / 33.15789...
    avg_price = engine.calculate_vwap(levels, 5000.0)
    expected_qty = 10 + 20 + (480.0 / 152.0)
    assert avg_price == pytest.approx(5000.0 / expected_qty)
