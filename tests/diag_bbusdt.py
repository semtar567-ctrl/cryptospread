import sys
import os
# Добавляем корень проекта в пути поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ccxt

def diagnose():
    print("[DIAG] Diagnosing BB/USDT market details (Synchronous CCXT)...")
    
    # Инициализируем биржи
    bybit = ccxt.bybit({'options': {'defaultType': 'swap'}})
    okx = ccxt.okx({'options': {'defaultType': 'swap'}})
    
    symbol = "BB/USDT:USDT"
    
    # 1. Диагностика Bybit
    try:
        print(f"\n[BYBIT] Loading market for {symbol}...")
        bybit.load_markets()
        if symbol in bybit.markets:
            m = bybit.markets[symbol]
            print(f"  Symbol: {m['symbol']}")
            print(f"  ID: {m['id']}")
            print(f"  Contract Size: {m.get('contractSize')} (multiplier: {m.get('type')})")
            print(f"  Lot Size Limits: {m['limits']['amount']}")
            print(f"  Price Precision: {m['precision']['price']}")
            
            ticker = bybit.fetch_ticker(symbol)
            print(f"  Ticker -> Bid: {ticker['bid']} | Ask: {ticker['ask']}")
        else:
            print(f"  [BYBIT] Symbol {symbol} not found in markets.")
    except Exception as e:
        print(f"[BYBIT ERROR] Failed: {e}")

    # 2. Диагностика OKX
    try:
        print(f"\n[OKX] Loading market for {symbol}...")
        okx.load_markets()
        if symbol in okx.markets:
            m = okx.markets[symbol]
            print(f"  Symbol: {m['symbol']}")
            print(f"  ID: {m['id']}")
            print(f"  Contract Size: {m.get('contractSize')} (lotSize: {m.get('lotSize')})")
            print(f"  Lot Size Limits: {m['limits']['amount']}")
            print(f"  Price Precision: {m['precision']['price']}")
            
            ticker = okx.fetch_ticker(symbol)
            print(f"  Ticker -> Bid: {ticker['bid']} | Ask: {ticker['ask']}")
        else:
            print(f"  [OKX] Symbol {symbol} not found in markets.")
    except Exception as e:
        print(f"[OKX ERROR] Failed: {e}")

if __name__ == "__main__":
    diagnose()
