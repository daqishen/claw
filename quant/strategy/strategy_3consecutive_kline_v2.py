#!/usr/bin python3
# -*- coding: utf-8 -*-
"""
三连K线交易策略 v2 - 成交量过滤版

策略逻辑：
- 第1根K线：成交量 > 前5根K线平均成交量的3倍，且价格突破前5根K线范围
- 突破最低点 + 后面2根收阳 -> 做多
- 突破最低点 + 后面2根收阴 -> 做空
- 突破最高点 + 后面2根收阴 -> 做空
- 突破最高点 + 后面2根收阳 -> 做多
- 止损：突破K线的最低价/最高价
- 止盈：1.5倍盈亏比
- 过滤开盘前3分钟、13:30-13:35、21:00-21:05
"""

import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, '/Users/qiyue/Desktop/test/claw/quant/strategy')
from future_base import FutureData, FutureTrader


CONTRACT_FILE = '/Users/qiyue/Desktop/test/claw/quant/data/history_futures/1min/FG2605_ZCE_1min.csv'
CONTRACT_NAME = 'FG2605'
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/res'


class ThreeConsecutiveStrategyV2:
    def __init__(self, data: FutureData, initial_capital: float = 1000000):
        self.data = data
        self.price_df = data.df.copy()
        if 'trade_time' in self.price_df.columns:
            self.price_df = self.price_df.sort_values('trade_time').reset_index(drop=True)
            self.time_col = 'trade_time'
        else:
            self.price_df = self.price_df.sort_values('time').reset_index(drop=True)
            self.time_col = 'time'
        
        self.trader = FutureTrader(data, initial_capital=initial_capital)
        self.signals = []
        self.trade_logs = []
    
    def is_bullish(self, df: pd.DataFrame, i: int) -> bool:
        if i >= len(df):
            return False
        return df.iloc[i]['close'] > df.iloc[i]['open']
    
    def is_bearish(self, df: pd.DataFrame, i: int) -> bool:
        if i >= len(df):
            return False
        return df.iloc[i]['close'] < df.iloc[i]['open']
    
    def check_breakout(self, df: pd.DataFrame, i: int) -> dict:
        if i < 6:
            return {'breakout': False, 'direction': None}
        
        prev_5 = df.iloc[i-6:i-1]
        curr = df.iloc[i]
        prev_mean = prev_5['vol'].mean()
        
        if curr['vol'] <= prev_mean * 3:
            return {'breakout': False, 'direction': None}
        
        if curr['low'] <= prev_5['low'].min():
            return {'breakout': True, 'direction': 'up'}
        if curr['high'] >= prev_5['high'].max():
            return {'breakout': True, 'direction': 'down'}
        
        return {'breakout': False, 'direction': None}
    
    def run(self):
        df = self.price_df.copy()
        
        print(f"数据共 {len(df)} 条")
        
        # 过滤时间
        df['hour'] = df[self.time_col].apply(lambda x: int(str(x)[11:13]))
        df['minute'] = df[self.time_col].apply(lambda x: int(str(x)[14:16]))
        
        # 过滤开盘前3分钟
        df = df[~((df['hour'] == 9) & (df['minute'] < 3))]
        
        print(f"过滤后 {len(df)} 条")
        
        for i in range(6, len(df)):
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
            
            # 禁止开仓时段
            hour = int(str(current_time)[11:13])
            minute = int(str(current_time)[14:16])
            if hour == 9 and minute < 5:
                continue
            if hour == 13 and minute < 35:
                continue
            if hour == 21 and minute < 5:
                continue
            
            open_positions = self.trader.trade_record.get_open_positions()
            
            # 做多条件
            if breakout_info['direction'] == 'up' and prev_bullish and curr_bullish:
                if len(open_positions) == 0:
                    stop_loss = kline1['low']
                    entry_price = df.iloc[i-1]['close']
                    take_profit = entry_price + (entry_price - stop_loss) * 1.5
                    risk = abs(entry_price - stop_loss)
                    
                    pos_id = self.trader.buy(
                        open_price=entry_price, volume=1, open_time=current_time,
                        stop_loss=stop_loss, take_profit=take_profit
                    )
                    self.signals.append({
                        'time': current_time, 'type': 'BUY', 'price': entry_price,
                        'stop_loss': stop_loss, 'take_profit': take_profit,
                        'risk': risk, 'target_rr': 1.5
                    })
                    self.trade_logs.append({
                        'time': current_time, 'action': 'OPEN_LONG',
                        'price': entry_price, 'stop_loss': stop_loss,
                        'take_profit': take_profit, 'position_id': pos_id
                    })
            
            elif breakout_info['direction'] == 'up' and prev_bearish and curr_bearish:
                if len(open_positions) == 0:
                    stop_loss = kline1['high']
                    entry_price = df.iloc[i-1]['close']
                    take_profit = entry_price - (stop_loss - entry_price) * 1.5
                    risk = abs(entry_price - stop_loss)
                    
                    pos_id = self.trader.short(
                        open_price=entry_price, volume=1, open_time=current_time,
                        stop_loss=stop_loss, take_profit=take_profit
                    )
                    self.signals.append({
                        'time': current_time, 'type': 'SELL', 'price': entry_price,
                        'stop_loss': stop_loss, 'take_profit': take_profit,
                        'risk': risk, 'target_rr': 1.5
                    })
                    self.trade_logs.append({
                        'time': current_time, 'action': 'OPEN_SHORT',
                        'price': entry_price, 'stop_loss': stop_loss,
                        'take_profit': take_profit, 'position_id': pos_id
                    })
            
            elif breakout_info['direction'] == 'down' and prev_bearish and curr_bearish:
                if len(open_positions) == 0:
                    stop_loss = kline1['high']
                    entry_price = df.iloc[i-1]['close']
                    take_profit = entry_price - (stop_loss - entry_price) * 1.5
                    risk = abs(entry_price - stop_loss)
                    
                    pos_id = self.trader.short(
                        open_price=entry_price, volume=1, open_time=current_time,
                        stop_loss=stop_loss, take_profit=take_profit
                    )
                    self.signals.append({
                        'time': current_time, 'type': 'SELL', 'price': entry_price,
                        'stop_loss': stop_loss, 'take_profit': take_profit,
                        'risk': risk, 'target_rr': 1.5
                    })
                    self.trade_logs.append({
                        'time': current_time, 'action': 'OPEN_SHORT',
                        'price': entry_price, 'stop_loss': stop_loss,
                        'take_profit': take_profit, 'position_id': pos_id
                    })
            
            elif breakout_info['direction'] == 'down' and prev_bullish and curr_bullish:
                if len(open_positions) == 0:
                    stop_loss = kline1['low']
                    entry_price = df.iloc[i-1]['close']
                    take_profit = entry_price + (entry_price - stop_loss) * 1.5
                    risk = abs(entry_price - stop_loss)
                    
                    pos_id = self.trader.buy(
                        open_price=entry_price, volume=1, open_time=current_time,
                        stop_loss=stop_loss, take_profit=take_profit
                    )
                    self.signals.append({
                        'time': current_time, 'type': 'BUY', 'price': entry_price,
                        'stop_loss': stop_loss, 'take_profit': take_profit,
                        'risk': risk, 'target_rr': 1.5
                    })
                    self.trade_logs.append({
                        'time': current_time, 'action': 'OPEN_LONG',
                        'price': entry_price, 'stop_loss': stop_loss,
                        'take_profit': take_profit, 'position_id': pos_id
                    })
            
            if len(open_positions) > 0:
                result = self.trader.process_price_update(current_price, current_time)
                for closed in result['closed_positions']:
                    self.trade_logs.append({
                        'time': current_time,
                        'action': f"CLOSE_{closed.get('reason', 'unknown').upper()}",
                        'price': current_price,
                        'profit': closed.get('profit', 0),
                        'position_id': closed.get('position_id')
                    })
        
        print("完成!")
    
    def get_results(self):
        trades_df = self.trader.trade_record.get_closed_trades()
        
        total_trades = len(trades_df)
        if total_trades == 0:
            return {'total_trades': 0}
        
        wins = len(trades_df[trades_df['profit'] > 0])
        losses = len(trades_df[trades_df['profit'] <= 0])
        win_rate = wins / total_trades * 100
        
        stop_loss_count = len(trades_df[trades_df['reason'] == 'stop_loss'])
        take_profit_count = len(trades_df[trades_df['reason'] == 'take_profit'])
        total_profit = trades_df['profit'].sum()
        
        # 计算止损距离和理论最优盈亏比
        price_df = self.price_df
        rr_list = []
        risk_list = []
        
        for _, row in trades_df.iterrows():
            open_time = row['open_time']
            close_time = row['close_time']
            direction = row['direction']
            entry_price = row['open_price']
            stop_price = row['stop_loss']
            
            # 止损距离
            risk = abs(entry_price - stop_price) / entry_price * 100  # 百分比
            risk_list.append(risk)
            
            # 最优盈亏比
            mask = (price_df[self.time_col] >= open_time) & (price_df[self.time_col] <= close_time)
            period = price_df[mask]
            if len(period) > 0:
                if direction == 'long':
                    best_price = period['high'].max()
                    reward = best_price - entry_price
                else:
                    best_price = period['low'].min()
                    reward = entry_price - best_price
                
                if risk > 0:
                    rr = reward / abs(entry_price - stop_price)
                else:
                    rr = 0
                rr_list.append(rr)
            else:
                rr_list.append(0)
        
        avg_risk = np.mean(risk_list) if risk_list else 0
        avg_rr = np.mean(rr_list) if rr_list else 0
        
        return {
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate,
            'stop_loss_count': stop_loss_count,
            'take_profit_count': take_profit_count,
            'total_profit': total_profit,
            'final_capital': self.trader.available_capital,
            'initial_capital': self.trader.initial_capital,
            'return_pct': (self.trader.available_capital - self.trader.initial_capital) / self.trader.initial_capital * 100,
            'avg_stop_loss_pct': avg_risk,
            'avg_optimal_rr': avg_rr
        }
    
    def save_results(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        signals_df = pd.DataFrame(self.signals)
        if len(signals_df) > 0:
            signals_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_v2_signals.csv', index=False, encoding='utf-8-sig')
        
        logs_df = pd.DataFrame(self.trade_logs)
        if len(logs_df) > 0:
            logs_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_v2_trade_logs.csv', index=False, encoding='utf-8-sig')
        
        trades_df = self.trader.trade_record.get_closed_trades()
        
        # 添加最优盈亏比
        if len(trades_df) > 0:
            rr_list = []
            for _, row in trades_df.iterrows():
                open_time = row['open_time']
                close_time = row['close_time']
                direction = row['direction']
                entry_price = row['open_price']
                stop_price = row['stop_loss']
                
                mask = (self.price_df[self.time_col] >= open_time) & (self.price_df[self.time_col] <= close_time)
                period = self.price_df[mask]
                
                risk = abs(entry_price - stop_price)
                
                if len(period) > 0 and risk > 0:
                    if direction == 'long':
                        best_price = period['high'].max()
                        reward = best_price - entry_price
                    else:
                        best_price = period['low'].min()
                        reward = entry_price - best_price
                    rr = reward / risk
                else:
                    rr = 0
                rr_list.append(rr)
            
            trades_df['optimal_rr'] = rr_list
        
        if len(trades_df) > 0:
            trades_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_v2_trades.csv', index=False, encoding='utf-8-sig')
        
        results = self.get_results()
        results_df = pd.DataFrame([results])
        results_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_v2_results.csv', index=False, encoding='utf-8-sig')
        
        return results


def main():
    print("=" * 60)
    print("三连K线策略 v2 - 成交量突破版")
    print("=" * 60)
    
    data = FutureData.from_csv(CONTRACT_FILE)
    print(f"加载数据: {len(data.df)} 条")
    
    strategy = ThreeConsecutiveStrategyV2(data, initial_capital=1000000)
    strategy.run()
    
    results = strategy.save_results()
    
    print("\n" + "=" * 60)
    print("回测结果")
    print("=" * 60)
    print(f"交易次数: {results['total_trades']}")
    print(f"胜率: {results['win_rate']:.2f}%")
    print(f"盈利次数: {results['wins']}")
    print(f"亏损次数: {results['losses']}")
    print(f"止损次数: {results['stop_loss_count']}")
    print(f"止盈次数: {results['take_profit_count']}")
    print(f"总收益: {results['total_profit']}")
    print(f"最终资金: {results['final_capital']}")
    print("-" * 60)
    print(f"平均止损距离: {results['avg_stop_loss_pct']:.4f}%")
    print(f"理论最优盈亏比: {results['avg_optimal_rr']:.2f}")
    print("=" * 60)


if __name__ == '__main__':
    main()
