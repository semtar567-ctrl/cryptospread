import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.diag_math_validator import calculate_vwap

def run_known_ground_truth_test():
    print("=" * 60)
    print("      DETERMINISTIC GROUND-TRUTH MATH TEST")
    print("=" * 60)

    # 1. Задаем точно известные ручные данные стаканов
    asks = [(100.0, 5.0), (102.0, 10.0)]
    bids = [(110.0, 4.0), (108.0, 10.0)]
    target_volume = 1000.0
    buy_fee = 0.0006
    sell_fee = 0.0006

    # 2. Выполняем расчет через алгоритм
    vwap_buy, usd_buy = calculate_vwap(asks, target_volume)
    vwap_sell, usd_sell = calculate_vwap(bids, target_volume)

    buy_price_best = asks[0][0] # 100.0
    sell_price_best = bids[0][0] # 110.0

    raw_spread_percent = ((sell_price_best - buy_price_best) / buy_price_best) * 100.0
    total_fee_percent = (buy_fee + sell_fee) * 2.0 * 100.0 # 0.24%
    raw_net_spread = raw_spread_percent - total_fee_percent

    vwap_spread_percent = ((vwap_sell - vwap_buy) / vwap_buy) * 100.0
    vwap_net_spread = vwap_spread_percent - total_fee_percent
    slippage = raw_net_spread - vwap_net_spread

    # 3. Ручные эталонные математические константы
    EXPECTED_VWAP_BUY = 100.99009900990099
    EXPECTED_VWAP_SELL = 108.87096774193549
    EXPECTED_RAW_NET_SPREAD = 9.760000000000000
    EXPECTED_VWAP_NET_SPREAD = 7.563605313092985
    EXPECTED_SLIPPAGE = 2.196394686907015

    print(f"  Best Buy Price:      ${buy_price_best:.2f}")
    print(f"  Best Sell Price:     ${sell_price_best:.2f}")
    print(f"  Working Volume:      ${target_volume:.2f}")
    print(f"-" * 60)
    print(f"  Calculated VWAP Buy:  ${vwap_buy:.8f} | Expected: ${EXPECTED_VWAP_BUY:.8f}")
    print(f"  Calculated VWAP Sell: ${vwap_sell:.8f} | Expected: ${EXPECTED_VWAP_SELL:.8f}")
    print(f"  Raw Net Spread:       {raw_net_spread:.6f}%        | Expected: {EXPECTED_RAW_NET_SPREAD:.6f}%")
    print(f"  VWAP Net Spread:      {vwap_net_spread:.6f}%        | Expected: {EXPECTED_VWAP_NET_SPREAD:.6f}%")
    print(f"  Slippage:             {slippage:.6f}%        | Expected: {EXPECTED_SLIPPAGE:.6f}%")
    print(f"=" * 60)

    # Проверки с точностью до 1e-9 (0.000000001)
    assert abs(vwap_buy - EXPECTED_VWAP_BUY) < 1e-9, f"VWAP Buy Mismatch: {vwap_buy}"
    assert abs(vwap_sell - EXPECTED_VWAP_SELL) < 1e-9, f"VWAP Sell Mismatch: {vwap_sell}"
    assert abs(raw_net_spread - EXPECTED_RAW_NET_SPREAD) < 1e-9, f"Raw Net Spread Mismatch: {raw_net_spread}"
    assert abs(vwap_net_spread - EXPECTED_VWAP_NET_SPREAD) < 1e-9, f"VWAP Net Spread Mismatch: {vwap_net_spread}"
    assert abs(slippage - EXPECTED_SLIPPAGE) < 1e-9, f"Slippage Mismatch: {slippage}"

    print("  [SUCCESS] 100.000000% DETERMINISTIC GROUND-TRUTH MATH MATCH!")

if __name__ == "__main__":
    run_known_ground_truth_test()
