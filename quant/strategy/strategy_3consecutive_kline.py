#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
三连K线交易策略

策略逻辑：
- 连续3根阳线 -> 买入做多，止损=最近一根阳线最低价，止盈=1:1盈亏比
- 连续3根阴线 -> 卖出做空，止损=最近一根阴线最高价，止盈=1:1盈亏比

使用方法：
    python3 strategy_3连续k线.py
"""

import sys
import os
import pandas as pd
import numpy as np

# 添加策略目录到路径
sys.path.insert(0, '/Users/qiyue/Desktop/test/claw/quant/strategy')
from future_base import FutureData, FutureTrader, ConditionalOrder


# ============== 策略配置 ==============
# 玻璃期货代码
CONTRACT_FILE = '/Users/qiyue/Desktop/test/claw/quant/data/history_futures/1min/FG2603_ZCE_1min.csv'  # 玻璃
CONTRACT_NAME = 'FG2603'  # 合约名称

# 输出目录
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/res'


class ThreeConsecutiveStrategy:
    """三连K线策略"""
    
    def __init__(self, data: FutureData, initial_capital: float = 1000000):
        """
        初始化
        
        Args:
            data: FutureData实例
            initial_capital: 初始资金
        """
        self.data = data
        self.trader = FutureTrader(data, initial_capital=initial_capital)
        
        # 策略信号记录
        self.signals = []
        # 交易日志
        self.trade_logs = []
    
    def is_bullish(self, df: pd.DataFrame, i: int) -> bool:
        """判断第i根K线是否为阳线"""
        if i >= len(df):
            return False
        return df.iloc[i]['close'] > df.iloc[i]['open']
    
    def is_bearish(self, df: pd.DataFrame, i: int) -> bool:
        """判断第i根K线是否为阴线"""
        if i >= len(df):
            return False
        return df.iloc[i]['close'] < df.iloc[i]['open']
    
    def get_low(self, df: pd.DataFrame, i: int) -> float:
        """获取第i根K线的最低价"""
        if i >= len(df):
            return None
        return df.iloc[i]['low']
    
    def get_high(self, df: pd.DataFrame, i: int) -> float:
        """获取第i根K线的最高价"""
        if i >= len(df):
            return None
        return df.iloc[i]['high']
    
    def get_close(self, df: pd.DataFrame, i: int) -> float:
        """获取第i根K线的收盘价"""
        if i >= len(df):
            return None
        return df.iloc[i]['close']
    
    def get_time(self, df: pd.DataFrame, i: int) -> str:
        """获取第i根K线的时间"""
        if i >= len(df):
            return None
        return df.iloc[i]['time']
    
    def run(self):
        """运行策略"""
        df = self.data.df.copy()
        df = df.sort_values('time').reset_index(drop=True)
        
        print(f"开始回测，数据共 {len(df)} 条")
        print("=" * 60)
        
        # 遍历K线
        for i in range(2, len(df)):
            # 获取最近3根K线
            kline1 = df.iloc[i-2]  # 第i-2根
            kline2 = df.iloc[i-1]  # 第i-1根
            kline3 = df.iloc[i]    # 最后一根
            
            current_time = kline3['time']
            current_price = kline3['close']
            
            # 检查是否有未平持仓
            open_positions = self.trader.trade_record.get_open_positions()
            
            # === 连续3根阳线 -> 买入做多 ===
            if (self.is_bullish(df, i-2) and 
                self.is_bullish(df, i-1) and 
                self.is_bullish(df, i)):
                
                # 如果没有持仓，则开多
                if len(open_positions) == 0:
                    # 止损 = 最近一根阳线最低价
                    stop_loss = self.get_low(df, i)
                    # 止盈 = 1:1盈亏比
                    entry_price = self.get_close(df, i)
                    take_profit = entry_price + (entry_price - stop_loss)
                    
                    # 开多仓
                    pos_id = self.trader.buy(
                        open_price=entry_price,
                        volume=1,
                        open_time=current_time,
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signal = {
                        'time': current_time,
                        'type': 'BUY',
                        'price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'kline_info': f"{kline1['close']} -> {kline2['close']} -> {kline3['close']}"
                    }
                    self.signals.append(signal)
                    
                    self.trade_logs.append({
                        'time': current_time,
                        'action': 'OPEN_LONG',
                        'price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'position_id': pos_id
                    })
            
            # === 连续3根阴线 -> 卖出做空 ===
            elif (self.is_bearish(df, i-2) and 
                  self.is_bearish(df, i-1) and 
                  self.is_bearish(df, i)):
                
                # 如果没有持仓，则开空
                if len(open_positions) == 0:
                    # 止损 = 最近一根阴线最高价
                    stop_loss = self.get_high(df, i)
                    # 止盈 = 1:1盈亏比
                    entry_price = self.get_close(df, i)
                    take_profit = entry_price - (stop_loss - entry_price)
                    
                    # 开空仓
                    pos_id = self.trader.short(
                        open_price=entry_price,
                        volume=1,
                        open_time=current_time,
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signal = {
                        'time': current_time,
                        'type': 'SELL',
                        'price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'kline_info': f"{kline1['close']} -> {kline2['close']} -> {kline3['close']}"
                    }
                    self.signals.append(signal)
                    
                    self.trade_logs.append({
                        'time': current_time,
                        'action': 'OPEN_SHORT',
                        'price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'position_id': pos_id
                    })
            
            # === 检查止损/止盈 ===
            if len(open_positions) > 0:
                result = self.trader.process_price_update(current_price, current_time)
                
                # 记录平仓信息
                for closed in result['closed_positions']:
                    reason = closed.get('reason', 'unknown')
                    self.trade_logs.append({
                        'time': current_time,
                        'action': f'CLOSE_{reason.upper()}',
                        'price': current_price,
                        'profit': closed.get('profit', 0),
                        'position_id': closed.get('position_id')
                    })
        
        print(f"回测完成!")
        print("=" * 60)
    
    def get_results(self) -> dict:
        """获取回测结果"""
        # 获取交易记录
        trades_df = self.trader.trade_record.get_closed_trades()
        
        # 统计
        total_trades = len(trades_df)
        if total_trades > 0:
            wins = len(trades_df[trades_df['profit'] > 0])
            losses = len(trades_df[trades_df['profit'] <= 0])
            win_rate = wins / total_trades * 100
            
            stop_loss_count = len(trades_df[trades_df['reason'] == 'stop_loss'])
            take_profit_count = len(trades_df[trades_df['reason'] == 'take_profit'])
            manual_count = len(trades_df[trades_df['reason'] == 'manual'])
            
            total_profit = trades_df['profit'].sum()
        else:
            wins = losses = win_rate = 0
            stop_loss_count = take_profit_count = manual_count = 0
            total_profit = 0
        
        return {
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate,
            'stop_loss_count': stop_loss_count,
            'take_profit_count': take_profit_count,
            'manual_count': manual_count,
            'total_profit': total_profit,
            'final_capital': self.trader.available_capital,
            'initial_capital': self.trader.initial_capital,
            'return_pct': (self.trader.available_capital - self.trader.initial_capital) / self.trader.initial_capital * 100
        }
    
    def save_results(self):
        """保存结果到文件"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 1. 保存交易信号
        signals_df = pd.DataFrame(self.signals)
        if len(signals_df) > 0:
            signals_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_signals.csv', index=False, encoding='utf-8-sig')
        
        # 2. 保存交易日志
        logs_df = pd.DataFrame(self.trade_logs)
        if len(logs_df) > 0:
            logs_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_trade_logs.csv', index=False, encoding='utf-8-sig')
        
        # 3. 保存交易记录
        trades_df = self.trader.trade_record.get_closed_trades()
        if len(trades_df) > 0:
            trades_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_trades.csv', index=False, encoding='utf-8-sig')
        
        # 4. 保存统计结果
        results = self.get_results()
        results_df = pd.DataFrame([results])
        results_df.to_csv(f'{OUTPUT_DIR}/{CONTRACT_NAME}_results.csv', index=False, encoding='utf-8-sig')
        
        print(f"\n结果已保存到 {OUTPUT_DIR}:")
        print(f"  - {CONTRACT_NAME}_signals.csv ({len(signals_df)} 条信号)")
        print(f"  - {CONTRACT_NAME}_trade_logs.csv ({len(logs_df)} 条日志)")
        print(f"  - {CONTRACT_NAME}_trades.csv ({len(trades_df)} 笔交易)")
        print(f"  - {CONTRACT_NAME}_results.csv (统计结果)")
        
        return results


def main():
    print("=" * 60)
    print("三连K线交易策略")
    print("=" * 60)
    print(f"合约: {CONTRACT_NAME}")
    print(f"数据: {CONTRACT_FILE}")
    print()
    
    # 加载数据
    data = FutureData.from_csv(CONTRACT_FILE)
    print(f"加载数据: {len(data.df)} 条")
    print(f"时间范围: {data.get_time_range()}")
    print()
    
    # 运行策略
    strategy = ThreeConsecutiveStrategy(data, initial_capital=1000000)
    strategy.run()
    
    # 保存结果
    results = strategy.save_results()
    
    # 打印结果
    print("\n" + "=" * 60)
    print("回测结果统计")
    print("=" * 60)
    print(f"初始资金: {results['initial_capital']}")
    print(f"最终资金: {results['final_capital']}")
    print(f"总收益率: {results['return_pct']:.2f}%")
    print(f"总交易次数: {results['total_trades']}")
    print(f"盈利次数: {results['wins']}")
    print(f"亏损次数: {results['losses']}")
    print(f"胜率: {results['win_rate']:.2f}%")
    print(f"止损次数: {results['stop_loss_count']}")
    print(f"止盈次数: {results['take_profit_count']}")
    print(f"手动平仓: {results['manual_count']}")
    print("=" * 60)


if __name__ == '__main__':
    main()
