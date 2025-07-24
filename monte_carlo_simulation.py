import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from tqdm import tqdm
import sys

# --- CONFIG ---
DATA_FILES = [
    ("OANDA_EURUSD, 15.csv", "EURUSD", "15m"),
    ("OANDA_EURUSD, 240.csv", "EURUSD", "4h"),
    ("OANDA_GBPUSD, 15.csv", "GBPUSD", "15m"),
    ("OANDA_GBPUSD, 240.csv", "GBPUSD", "4h"),
]
N_RUNS = 1000
SLIPPAGE_PIPS = 1  # Simulate 1 pip random slippage per trade
RISK_PER_TRADE = 0.01  # 1% risk per trade
ATR_PERIOD = 14

# --- Helper functions ---
def load_data(filename):
    df = pd.read_csv(filename)
    # Try to parse time column
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], unit='s')
    return df

def simulate_trades(df, symbol, timeframe, override_logic, n_runs=1000):
    results = []
    for run in tqdm(range(n_runs), desc=f"{symbol} {timeframe}"):
        trades = []
        position = None
        entry_price = None
        sl = None
        tp = None
        atr = None
        for i in range(ATR_PERIOD, len(df)):
            row = df.iloc[i]
            # Entry logic: simple example, enter on every bar
            if position is None:
                entry_price = row['close']
                atr = row['ATR'] if 'ATR' in row else df['close'].rolling(ATR_PERIOD).std().iloc[i]
                sl = entry_price - atr * 1.0  # 1R risk
                tp = entry_price + atr * 2.0  # 2R reward (old logic)
                position = {
                    'entry': entry_price,
                    'sl': sl,
                    'tp': tp,
                    'open_idx': i,
                    'override_fired': False
                }
                continue
            # Simulate price movement
            low = row['low']
            high = row['high']
            # Check for stop loss or take profit
            exit_reason = None
            if low <= position['sl']:
                exit_price = position['sl']
                exit_reason = 'SL'
            elif high >= position['tp']:
                exit_price = position['tp']
                exit_reason = 'TP'
            else:
                # Simulate CLOSE signal every 10 bars
                if (i - position['open_idx']) % 10 == 0:
                    # --- Override logic ---
                    if override_logic and not position['override_fired']:
                        # Example: fire override if profit > 1.5R and bar count < 8
                        profit = (row['close'] - position['entry']) / (position['entry'] - position['sl'])
                        if profit > 1.5 and (i - position['open_idx']) < 8:
                            # Widen TP, move SL
                            position['sl'] = row['close'] - atr * 1.5
                            position['tp'] = row['close'] + atr * (1.5 if timeframe == '15m' else 2.0)
                            position['override_fired'] = True
                            continue
                    # Otherwise, exit on CLOSE
                    exit_price = row['close']
                    exit_reason = 'CLOSE'
                else:
                    continue
            # Apply slippage
            slip = np.random.uniform(-SLIPPAGE_PIPS, SLIPPAGE_PIPS) * 0.0001
            exit_price += slip
            r = (exit_price - position['entry']) / (position['entry'] - position['sl'])
            trades.append(r)
            position = None
        # End of data: close any open position
        if position is not None:
            exit_price = df.iloc[-1]['close']
            r = (exit_price - position['entry']) / (position['entry'] - position['sl'])
            trades.append(r)
        results.append(trades)
    return results

def summarize_results(results, label):
    all_trades = np.concatenate([np.array(r) for r in results if len(r) > 0])
    mean_r = np.mean(all_trades)
    win_rate = np.mean(all_trades > 0)
    print(f"{label}: Mean R/trade = {mean_r:.3f}, Win rate = {win_rate:.2%}, Trades = {len(all_trades)}")
    return mean_r, win_rate, len(all_trades)

def main():
    try:
        print("Starting Monte Carlo simulation...")
        summary = []
        for filename, symbol, timeframe in DATA_FILES:
            print(f"\n=== {symbol} {timeframe} ===")
            if not os.path.exists(filename):
                print(f"File not found: {filename}")
                continue
            try:
                df = load_data(filename)
                print(f"Loaded {len(df)} rows from {filename}")
                # Old logic: always exit on CLOSE
                old_results = simulate_trades(df, symbol, timeframe, override_logic=False, n_runs=N_RUNS)
                print(f"Old logic simulation complete for {symbol} {timeframe}")
                # New logic: allow override
                new_results = simulate_trades(df, symbol, timeframe, override_logic=True, n_runs=N_RUNS)
                print(f"New logic simulation complete for {symbol} {timeframe}")
                # Collect summary using summarize_results
                old_mean_r, old_win_rate, old_trades = summarize_results(old_results, f"Old ({symbol} {timeframe})")
                new_mean_r, new_win_rate, new_trades = summarize_results(new_results, f"New ({symbol} {timeframe})")
                summary.append({
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'old_mean_r': old_mean_r,
                    'old_win_rate': old_win_rate,
                    'old_trades': old_trades,
                    'new_mean_r': new_mean_r,
                    'new_win_rate': new_win_rate,
                    'new_trades': new_trades,
                })
            except Exception as e:
                print(f"Error processing {filename}: {e}", file=sys.stderr)
        if summary:
            import csv
            with open('montecarlo_summary.csv', 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=summary[0].keys())
                writer.writeheader()
                writer.writerows(summary)
            print("Summary written to montecarlo_summary.csv")
        else:
            print("No summary to write.")
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
    finally:
        print(f"Current working directory: {os.getcwd()}")
        print("Files in directory:", os.listdir(os.getcwd()))

if __name__ == "__main__":
    main()
