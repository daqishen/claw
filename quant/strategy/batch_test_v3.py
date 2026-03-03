#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量测试所有主力合约的三连K线策略
"""

import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, '/Users/qiyue/Desktop/test/claw/quant/strategy')
from future_base import FutureData


DATA_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_futures/1min'
MAIN_CONTRACTS_FILE = '/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv'
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/res'


class StrategyV3:
    def __init__(self, data, initial_capital=1000000):
        self.data = data
        self.price_df = data.df.copy()
        if 'trade_time' in self.price_df.columns:
            self.price_df = self.price_df.sort_values('trade_time').reset_index(drop=True)
            self.time_col = 'trade_time'
        else:
            self.price_df = self.price_df.sort_values('time').reset_index(drop=True)
            self.time_col = 'time'

        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions = []
        self.trades = []
        self.signals = []

    def is_bullish(self, df, i):
        if i >= len(df):
            return False
        return df.iloc[i]['close'] > df.iloc[i]['open']

    def is_bearish(self, df, i):
        if i >= len(df):
            return False
        return df.iloc[i]['close'] < df.iloc[i]['open']

    def check_breakout(self, df, i):
        """检查放量突破 - 近15根K判定"""
        if i < 16:
            return {'breakout': False, 'direction': None}

        prev_15 = df.iloc[i-16:i-1]
        curr = df.iloc[i]

        if curr['vol'] <= prev_15['vol'].mean() * 3:
            return {'breakout': False, 'direction': None}

        if curr['low'] <= prev_15['low'].min():
            return {'breakout': True, 'direction': 'up'}
        if curr['high'] >= prev_15['high'].max():
            return {'breakout': True, 'direction': 'down'}

        return {'breakout': False, 'direction': None}

    def open_position(self, direction, entry_price, stop_loss, open_time):
        volume = 10
        margin = volume * entry_price * 50 * 0.1
        
        if margin > self.capital * 0.5:
            volume = int((self.capital * 0.5) / (entry_price * 50 * 0.1))
            volume = max(1, volume)
            margin = volume * entry_price * 50 * 0.1

        risk = abs(entry_price - stop_loss)
        tp1_price = entry_price + risk * 1 if direction == 'long' else entry_price - risk * 1
        tp2_price = entry_price + risk * 2 if direction == 'long' else entry_price - risk * 2

        position = {
            'direction': direction,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'original_stop': stop_loss,
            'tp1_price': tp1_price,
            'tp2_price': tp2_price,
            'volume': volume,
            'margin': margin,
            'open_time': open_time,
            'half_closed': False,
            'status': 'open'
        }

        self.positions.append(position)
        self.capital -= margin

        return position

    def close_position(self, idx, close_price, close_time, reason, volume=None, current_stop_loss=None):
        pos = self.positions[idx]
        if pos['status'] != 'open':
            return None

        if volume is None:
            volume = pos['volume']

        volume = min(volume, pos['volume'])

        if pos['direction'] == 'long':
            profit = (close_price - pos['entry_price']) * volume
        else:
            profit = (pos['entry_price'] - close_price) * volume

        margin_return = volume * pos['entry_price'] * 50 * 0.1
        self.capital += margin_return + profit

        actual_stop = current_stop_loss if current_stop_loss is not None else pos.get('original_stop', pos.get('stop_loss'))

        self.trades.append({
            'direction': pos['direction'],
            'open_price': pos['entry_price'],
            'close_price': close_price,
            'volume': volume,
            'open_time': pos['open_time'],
            'close_time': close_time,
            'stop_loss': actual_stop,
            'take_profit_1': pos.get('tp1_price'),
            'take_profit_2': pos.get('tp2_price'),
            'profit': profit,
            'return_pct': profit / (pos['entry_price'] * volume * 50 * 0.1) * 100,
            'reason': reason,
            'status': 'closed'
        })

        pos['volume'] -= volume
        if pos['volume'] <= 0:
            pos['status'] = 'closed'

        return profit

    def run(self):
        df = self.price_df.copy()

        # 过滤时间
        df['hour'] = df[self.time_col].apply(lambda x: int(str(x)[11:13]))
        df['minute'] = df[self.time_col].apply(lambda x: int(str(x)[14:16]))
        df = df[~((df['hour'] == 9) & (df['minute'] < 3))]

        for i in range(18, len(df)):
            breakout_info = self.check_breakout(df, i-2)
            if not breakout_info['breakout']:
                continue

            prev_bullish = self.is_bullish(df, i-1)
            prev_bearish = self.is_bearish(df, i-1)
            curr_bullish = self.is_bullish(df, i)
            curr_bearish = self.is_bearish(df, i)

            kline1 = df.iloc[i-2]
            current_time = df.iloc[i][self.time_col]
            current_price = df.iloc[i]['close']

            hour = int(str(current_time)[11:13])
            minute = int(str(current_time)[14:16])
            if hour == 9 and minute < 5:
                continue
            if hour == 13 and minute < 35:
                continue
            if hour == 21 and minute < 5:
                continue

            has_position = any(p['status'] == 'open' for p in self.positions)

            if not has_position:
                if breakout_info['direction'] == 'up' and prev_bullish and curr_bullish:
                    stop_loss = kline1['low']
                    entry_price = df.iloc[i-1]['close']
                    pos = self.open_position('long', entry_price, stop_loss, current_time)
                    self.signals.append({'time': current_time, 'type': 'BUY', 'price': entry_price, 'stop_loss': stop_loss, 'volume': pos['volume']})

                elif breakout_info['direction'] == 'up' and prev_bearish and curr_bearish:
                    stop_loss = kline1['high']
                    entry_price = df.iloc[i-1]['close']
                    pos = self.open_position('short', entry_price, stop_loss, current_time)
                    self.signals.append({'time': current_time, 'type': 'SELL', 'price': entry_price, 'stop_loss': stop_loss, 'volume': pos['volume']})

                elif breakout_info['direction'] == 'down' and prev_bearish and curr_bearish:
                    stop_loss = kline1['high']
                    entry_price = df.iloc[i-1]['close']
                    pos = self.open_position('short', entry_price, stop_loss, current_time)
                    self.signals.append({'time': current_time, 'type': 'SELL', 'price': entry_price, 'stop_loss': stop_loss, 'volume': pos['volume']})

                elif breakout_info['direction'] == 'down' and prev_bullish and curr_bullish:
                    stop_loss = kline1['low']
                    entry_price = df.iloc[i-1]['close']
                    pos = self.open_position('long', entry_price, stop_loss, current_time)
                    self.signals.append({'time': current_time, 'type': 'BUY', 'price': entry_price, 'stop_loss': stop_loss, 'volume': pos['volume']})

            for idx, pos in enumerate(self.positions):
                if pos['status'] != 'open':
                    continue

                if pos['direction'] == 'long':
                    profit = (current_price - pos['entry_price'])
                    risk = pos['entry_price'] - pos['stop_loss']
                else:
                    profit = (pos['entry_price'] - current_price)
                    risk = pos['stop_loss'] - pos['entry_price']

                if risk <= 0:
                    rr = 0
                else:
                    rr = profit / risk

                if rr >= 1.0 and not pos.get('half_closed', False):
                    half_vol = pos['volume'] // 2
                    if half_vol > 0:
                        self.close_position(idx, current_price, current_time, 'rr_1', half_vol, current_stop_loss=pos['stop_loss'])
                        pos['half_closed'] = True

                elif rr >= 2.0:
                    self.close_position(idx, current_price, current_time, 'rr_2', current_stop_loss=pos['stop_loss'])

                elif (pos['direction'] == 'long' and current_price <= pos['stop_loss']) or \
                     (pos['direction'] == 'short' and current_price >= pos['stop_loss']):
                    sl_price = pos['stop_loss']
                    self.close_position(idx, sl_price, current_time, 'stop_loss', current_stop_loss=sl_price)

        if len(df) > 0:
            last_time = df.iloc[-1][self.time_col]
            last_price = df.iloc[-1]['close']
            for idx, pos in enumerate(self.positions):
                if pos['status'] == 'open':
                    self.close_position(idx, last_price, last_time, 'end', current_stop_loss=pos['stop_loss'])

    def get_results(self):
        trades_df = pd.DataFrame(self.trades)
        if len(trades_df) == 0:
            return {'total_trades': 0}

        total_trades = len(trades_df)
        wins = len(trades_df[trades_df['profit'] > 0])
        losses = len(trades_df[trades_df['profit'] <= 0])
        win_rate = wins / total_trades * 100

        stop_loss_count = len(trades_df[trades_df['reason'] == 'stop_loss'])
        rr_1_count = len(trades_df[trades_df['reason'] == 'rr_1'])
        rr_2_count = len(trades_df[trades_df['reason'] == 'rr_2'])

        total_profit = trades_df['profit'].sum()

        return {
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate,
            'stop_loss_count': stop_loss_count,
            'rr_1_count': rr_1_count,
            'rr_2_count': rr_2_count,
            'total_profit': total_profit,
            'final_capital': self.capital,
            'initial_capital': self.initial_capital,
            'return_pct': (self.capital - self.initial_capital) / self.initial_capital * 100
        }


def get_contract_filename(ts_code):
    """转换合约代码为文件名"""
    # 例如: FG2605.ZCE -> FG2605_ZCE_1min.csv
    name = ts_code.replace('.', '_')
    return f"{name}_1min.csv"


def run_strategy_for_contract(filepath, contract_name):
    """对单个合约运行策略"""
    try:
        data = FutureData.from_csv(filepath)
        if len(data.df) < 100:
            return None
        
        strategy = StrategyV3(data, initial_capital=1000000)
        strategy.run()
        
        results = strategy.get_results()
        results['contract'] = contract_name
        
        # 保存结果
        trades_df = pd.DataFrame(strategy.trades)
        if len(trades_df) > 0:
            trades_df.to_csv(f'{OUTPUT_DIR}/{contract_name}_v3_trades.csv', index=False, encoding='utf-8-sig')
        
        return results
    except Exception as e:
        print(f"  错误: {e}")
        return None


def main():
    print("=" * 70)
    print("批量测试所有主力合约 - 三连K线策略 v3 (15根K)")
    print("=" * 70)
    
    # 读取主力合约列表
    main_df = pd.read_csv(MAIN_CONTRACTS_FILE)
    print(f"\n共有 {len(main_df)} 个主力合约")
    
    results = []
    success = 0
    fail = 0
    
    for i, row in main_df.iterrows():
        ts_code = row['ts_code']
        fut_code = row['fut_code']
        contract_name = ts_code.replace('.', '_')
        
        filename = get_contract_filename(ts_code)
        filepath = os.path.join(DATA_DIR, filename)
        
        print(f"[{i+1}/{len(main_df)}] {fut_code} ({ts_code})...", end=" ", flush=True)
        
        if not os.path.exists(filepath):
            print(f"文件不存在")
            fail += 1
            continue
        
        result = run_strategy_for_contract(filepath, contract_name)
        
        if result and result.get('total_trades', 0) > 0:
            print(f"交易:{result['total_trades']} 胜率:{result['win_rate']:.1f}% 收益:{result['total_profit']:.0f}")
            results.append(result)
            success += 1
        else:
            print("无交易")
            fail += 1
    
    # 汇总结果
    print("\n" + "=" * 70)
    print("汇总结果")
    print("=" * 70)
    
    if results:
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('total_profit', ascending=False)
        
        print(f"\n成功测试: {success} 个合约")
        print(f"失败/无交易: {fail} 个合约")
        
        total_trades = results_df['total_trades'].sum()
        total_profit = results_df['total_profit'].sum()
        avg_win_rate = results_df['win_rate'].mean()
        
        print(f"\n总计:")
        print(f"  交易次数: {total_trades}")
        print(f"  平均胜率: {avg_win_rate:.2f}%")
        print(f"  总收益: {total_profit:.0f} 元")
        
        # 保存汇总
        results_df.to_csv(f'{OUTPUT_DIR}/all_contracts_v3_summary.csv', index=False, encoding='utf-8-sig')
        
        # 显示前10名
        print("\n" + "-" * 70)
        print("收益前10名:")
        print("-" * 70)
        for i, row in results_df.head(10).iterrows():
            print(f"  {row['contract']:20} 交易:{row['total_trades']:3} 胜率:{row['win_rate']:5.1f}% 收益:{row['total_profit']:8.0f}")
        
        # 显示后10名
        print("\n" + "-" * 70)
        print("收益后10名:")
        print("-" * 70)
        for i, row in results_df.tail(10).iterrows():
            print(f"  {row['contract']:20} 交易:{row['total_trades']:3} 胜率:{row['win_rate']:5.1f}% 收益:{row['total_profit']:8.0f}")
    else:
        print("没有成功的测试结果")


if __name__ == '__main__':
    main()
