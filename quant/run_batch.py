import pandas as pd
import sys
import os
sys.path.insert(0, 'strategy')

from strategy_3consecutive_kline_v3 import ThreeConsecutiveKlineStrategy

DATA_DIR = 'data/history_futures/1min'
MAIN_FILE = 'main_contracts.csv'

main_df = pd.read_csv(MAIN_FILE)
results = []

for i, row in main_df.iterrows():
    ts_code = row['ts_code']
    fut_code = row['fut_code']
    filename = ts_code.replace('.', '_') + '_1min.csv'
    filepath = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(filepath):
        continue
    
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        if len(df) < 100:
            continue
        
        df = df.sort_values('trade_time').reset_index(drop=True)
        strategy = ThreeConsecutiveKlineStrategy({'lookback_bars': 15, 'volume_multiplier': 3})
        signals = strategy.check_signals(df)
        
        capital = 1000000
        for signal in signals:
            entry_idx = None
            for j, r in df.iterrows():
                if r['trade_time'] == signal['time']:
                    entry_idx = j
                    break
            
            if entry_idx is None or entry_idx >= len(df) - 1:
                continue
            
            direction = signal['direction']
            entry_price = signal['entry_price']
            stop_loss = signal['stop_loss']
            tp2 = signal['take_profit_2']
            volume = 10
            
            profit = None
            for k in range(entry_idx + 1, len(df)):
                kline = df.iloc[k]
                if direction == 'long':
                    if kline['low'] <= stop_loss:
                        profit = (stop_loss - entry_price) * volume
                        break
                    elif kline['high'] >= tp2:
                        profit = (tp2 - entry_price) * volume
                        break
                else:
                    if kline['high'] >= stop_loss:
                        profit = (entry_price - stop_loss) * volume
                        break
                    elif kline['low'] <= tp2:
                        profit = (entry_price - tp2) * volume
                        break
            
            if profit is not None:
                capital += profit
        
        total_profit = capital - 1000000
        results.append({'name': fut_code, 'trades': len(signals), 'profit': total_profit})
        print(f'{fut_code}: {len(signals)} {total_profit}')
    except Exception as e:
        print(f'{fut_code}: Error - {e}')

results_df = pd.DataFrame(results).sort_values('profit', ascending=False)
results_df.to_csv('res/all_contracts_v3_all.csv', index=False)
print(f'\nTotal: {results_df["trades"].sum()} trades, Profit: {results_df["profit"].sum()}')
