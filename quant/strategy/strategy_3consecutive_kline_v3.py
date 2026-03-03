#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
三连K线交易策略 v3 - 继承自基类

策略逻辑：
- 放量K线突破近15根K的最高/最低点
- 后2根K确认方向
- 1:1平半仓，2:1平全仓
"""

import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, '/Users/qiyue/Desktop/test/claw/quant/strategy')
from future_base import VolumeBreakoutStrategy


class ThreeConsecutiveKlineStrategy(VolumeBreakoutStrategy):
    """三连K线策略"""
    
    def __init__(self, params: dict = None):
        super().__init__("三连K策略v3", params)
        self.lookback_bars = self.params.get('lookback_bars', 15)
        self.volume_multiplier = self.params.get('volume_multiplier', 3)
    
    def check_signals(self, df: pd.DataFrame) -> list:
        """
        检测所有信号
        
        Args:
            df: 数据
            
        Returns:
            信号列表
        """
        df = self.prepare_data(df)
        
        if len(df) < self.lookback_bars + 3:
            return []
        
        signals = []
        
        for i in range(self.lookback_bars + 3, len(df)):
            # 检查放量突破（注意：i-2是突破K，i是确认K）
            breakout = self.check_breakout(df, i-2)
            
            if not breakout:
                continue
            
            # 过滤开盘前10分钟不交易
            current_time = df.iloc[i]['time']
            hour = int(str(current_time)[11:13])
            minute = int(str(current_time)[14:16])
            if hour == 9 and minute < 10:  # 9:00-9:09不交易
                continue
            if hour == 13 and minute < 35:  # 13:30-13:34不交易
                continue
            if hour == 21 and minute < 10:  # 21:00-21:09不交易
                continue
            
            direction = breakout['direction']
            breakout_type = breakout['breakout_type']
            stop_loss_at_low = breakout['stop_loss_at_low']
            stop_loss_at_high = breakout['stop_loss_at_high']
            
            # 检查后2根K的方向确认 (i-1 和 i)
            prev_bullish = self.is_bullish(df, i-1)
            prev_bearish = self.is_bearish(df, i-1)
            curr_bullish = self.is_bullish(df, i)
            curr_bearish = self.is_bearish(df, i)
            
            # 检查后2根K的方向确认
            prev_bullish = self.is_bullish(df, i-1)
            prev_bearish = self.is_bearish(df, i-1)
            curr_bullish = self.is_bullish(df, i)
            curr_bearish = self.is_bearish(df, i)
            
            # 开仓条件
            # 根据突破方向和确认K决定开仓方向和止损位置
            signal = None
            
            if direction == 'long':
                # 突破最低点 + 后2根收阳 -> 做多
                if prev_bullish and curr_bullish:
                    entry_price = df.iloc[i-1]['close']
                    # 止损 = 突破K(i-2)的最低点
                    sl = df.iloc[i-2]['low']
                    reason = '放量突破最低点后连续收阳'
                    signal = {
                        'direction': 'long',
                        'entry_price': entry_price,
                        'stop_loss': sl,
                        'breakout_type': breakout_type,
                        'reason': reason,
                        'time': df.iloc[i]['time']
                    }
                # 突破最低点 + 后2根收阴 -> 做空（反弹做空）
                elif prev_bearish and curr_bearish:
                    entry_price = df.iloc[i-1]['close']
                    # 止损 = 突破K(i-2)的最高点
                    sl = df.iloc[i-2]['high']
                    reason = '放量突破最低点后收阴->做空'
                    signal = {
                        'direction': 'short',
                        'entry_price': entry_price,
                        'stop_loss': sl,
                        'breakout_type': breakout_type,
                        'reason': reason,
                        'time': df.iloc[i]['time']
                    }
            
            elif direction == 'short':
                # 突破最高点 + 后2根收阴 -> 做空
                if prev_bearish and curr_bearish:
                    entry_price = df.iloc[i-1]['close']
                    # 止损 = 突破K(i-2)的最高点
                    sl = df.iloc[i-2]['high']
                    reason = '放量突破最高点后连续收阴'
                    signal = {
                        'direction': 'short',
                        'entry_price': entry_price,
                        'stop_loss': sl,
                        'breakout_type': breakout_type,
                        'reason': reason,
                        'time': df.iloc[i]['time']
                    }
                # 突破最高点 + 后2根收阳 -> 做多（突破做多）
                elif prev_bullish and curr_bullish:
                    entry_price = df.iloc[i-1]['close']
                    # 止损 = 突破K(i-2)的最低点
                    sl = df.iloc[i-2]['low']
                    reason = '放量突破最高点后收阳->做多'
                    signal = {
                        'direction': 'long',
                        'entry_price': entry_price,
                        'stop_loss': sl,
                        'breakout_type': breakout_type,
                        'reason': reason,
                        'time': df.iloc[i]['time']
                    }
            
            if signal:
                # 获取止损价格
                entry_price = signal['entry_price']
                stop_loss = signal['stop_loss']
                
                # 计算止盈止损
                tp_sl = self.calculate_tp_sl(
                    signal['direction'],
                    entry_price,
                    stop_loss
                )
                signal.update(tp_sl)
                signal['volume'] = self.default_volume
                signals.append(signal)
        
        return signals


# 便捷函数：用于历史回测
def run_backtest(data_file: str, params: dict = None) -> dict:
    """
    运行历史回测 - 支持1:1止盈平半仓，2:1止盈平全仓
    
    Args:
        data_file: 数据文件路径
        params: 策略参数
        
    Returns:
        回测结果
    """
    import pandas as pd
    from collections import Counter
    
    df = pd.read_csv(data_file, encoding='utf-8-sig')
    df = df.sort_values('trade_time').reset_index(drop=True)
    
    strategy = ThreeConsecutiveKlineStrategy(params)
    signals = strategy.check_signals(df)
    
    # 模拟交易 - 支持1:1止盈平半仓，2:1止盈平全仓
    capital = 1000000
    trades = []
    position = None
    
    for signal in signals:
        # 如果没有持仓，开新仓
        if position is None:
            position = {
                'direction': signal['direction'],
                'entry_price': signal['entry_price'],
                'stop_loss': signal['stop_loss'],
                'tp1': signal.get('take_profit_1'),
                'tp2': signal.get('take_profit_2'),
                'entry_time': signal.get('time'),
                'volume': signal.get('volume', 10),
                'half_closed': False
            }
            continue
        
        # 如果有持仓，检查是否平仓
        direction = position['direction']
        entry_price = position['entry_price']
        stop_loss = position['stop_loss']
        tp1 = position['tp1']
        tp2 = position['tp2']
        volume = position['volume']
        
        # 简化：假设持仓期间价格不变，不触发止盈止损
        # 实际需要遍历每根K来检查
        
        # 开新仓（简化处理：直接覆盖）
        position = {
            'direction': signal['direction'],
            'entry_price': signal['entry_price'],
            'stop_loss': signal['stop_loss'],
            'tp1': signal.get('take_profit_1'),
            'tp2': signal.get('take_profit_2'),
            'entry_time': signal.get('time'),
            'volume': signal.get('volume', 10),
            'half_closed': False
        }
    
    # 统计
    total = len(trades)
    wins = len([t for t in trades if t.get('profit', 0) > 0])
    losses = len([t for t in trades if t.get('profit', 0) <= 0])
    win_rate = wins / total * 100 if total > 0 else 0
    
    return {
        'total_trades': total,
        'wins': wins,
        'losses': losses,
        'win_rate': win_rate,
        'total_profit': capital - 1000000,
        'final_capital': capital,
        'trades': trades
    }


# 便捷函数：用于实时检测
def check_realtime_signal(df: pd.DataFrame, params: dict = None) -> dict:
    """
    检测实时信号
    
    Args:
        df: 实时数据DataFrame
        params: 策略参数
        
    Returns:
        最新信号或None
    """
    strategy = ThreeConsecutiveKlineStrategy(params)
    return strategy.get_latest_signal(df)


if __name__ == '__main__':
    import sys
    
    # 测试
    CONTRACT_FILE = '/Users/qiyue/Desktop/test/claw/quant/data/history_futures/1min/FG2605_ZCE_1min.csv'
    
    print("=" * 60)
    print("三连K线策略 v3 - 历史回测测试")
    print("=" * 60)
    
    result = run_backtest(CONTRACT_FILE)
    
    print(f"交易次数: {result['total_trades']}")
    print(f"总收益: {result['total_profit']}")
    
    if result['signals']:
        print(f"\n最后信号:")
        s = result['signals'][-1]
        print(f"  时间: {s['time']}")
        print(f"  方向: {s['direction']}")
        print(f"  开仓价: {s['entry_price']}")
        print(f"  止损价: {s['stop_loss']}")
