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
    
    # 各品种的最小止损tick数
    MIN_STOP_LOSS_TICKS = {
        'FG': 4,   # 玻璃
        'SA': 4,   # 纯碱
        'RB': 2,   # 螺纹钢
        'AU': 2,   # 沪金
        'AG': 2,   # 白银
        'CU': 2,   # 铜
        'AL': 2,   # 铝
        'ZN': 2,   # 锌
        'PB': 2,   # 铅
        'NI': 2,   # 镍
        'RU': 2,   # 橡胶
        'TA': 4,   # PTA
        'MA': 4,   # 甲醇
        'ZC': 4,   # 动力煤
        'CF': 4,   # 棉花
        'SR': 4,   # 白糖
        'RM': 4,   # 菜粕
        'OI': 4,   # 菜油
        'J': 2,    # 焦炭
        'JM': 2,   # 焦煤
        'I': 2,    # 铁矿石
        'J': 2,    # 螺纹钢
        'HC': 2,   # 热卷
    }
    
    def __init__(self, params: dict = None):
        super().__init__("三连K策略v3", params)
        self.lookback_bars = self.params.get('lookback_bars', 15)
        self.volume_multiplier = self.params.get('volume_multiplier', 3)
        self.min_stop_loss_ticks = self.params.get('min_stop_loss_ticks', 2)  # 默认最小2tick
    
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
                
                # 止盈止损距离必须 >= 最小报价变动 x 2
                # 例如螺纹钢报价87，最小变动1，那么止盈止损距离必须>=2
                # 如果计算出来止盈=86,止损=88，距离=1<2，就不开单
                risk = abs(entry_price - stop_loss)
                
                # 获取品种代码
                ts_code = df.iloc[i].get('ts_code', '')
                variety = ''.join([c for c in str(ts_code) if c.isalpha()]) if ts_code else ''
                
                # 获取该品种的最小报价变动(tick size)
                tick_size = self._get_tick_size(variety)
                min_risk = tick_size * 2  # 必须 >= 2倍最小变动
                
                if risk < min_risk:
                    # 止盈止损距离太小，跳过
                    continue
                
                # 计算止盈止损
                tp_sl = self.calculate_tp_sl(
                    signal['direction'],
                    entry_price,
                    stop_loss
                )
                
                # 额外检查：止盈距离也要 >= 2倍最小变动
                tp1 = tp_sl.get('take_profit_1')
                if tp1:
                    tp_distance = abs(entry_price - tp1)
                    if tp_distance < min_risk:
                        continue
                
                signal.update(tp_sl)
                signal['volume'] = self.default_volume
                signals.append(signal)
        
        return signals
    
    def _get_tick_size(self, variety: str) -> float:
        """获取品种的tick大小"""
        tick_sizes = {
            'FG': 1,   # 玻璃 1元
            'SA': 1,   # 纯碱 1元
            'RB': 1,   # 螺纹钢 1元
            'AU': 0.05, # 沪金 0.05元/克
            'AG': 1,   # 白银 1元/千克
            'CU': 10,  # 铜 10元/吨
            'AL': 5,   # 铝 5元/吨
            'ZN': 5,   # 锌 5元/吨
            'PB': 5,   # 铅 5元/吨
            'NI': 10,  # 镍 10元/吨
            'RU': 5,   # 橡胶 5元/吨
            'TA': 2,   # PTA 2元/吨
            'MA': 1,   # 甲醇 1元/吨
            'ZC': 0.4, # 动力煤 0.4元/吨
            'CF': 5,   # 棉花 5元/吨
            'SR': 1,   # 白糖 1元/吨
            'RM': 1,   # 菜粕 1元/吨
            'OI': 1,   # 菜油 1元/吨
            'J': 0.5,  # 焦炭 0.5元/吨
            'JM': 0.5, # 焦煤 0.5元/吨
            'I': 0.5,  # 铁矿石 0.5元/吨
            'HC': 1,   # 热卷 1元/吨
            'PP': 1,   # PP 1元/吨
            'PVC': 1,  # PVC 1元/吨
            'L': 1,    # 塑料 1元/吨
            'V': 1,    # PVC 1元/吨
            'EG': 1,   # 乙二醇 1元/吨
            'SC': 0.1, # 原油 0.1元/桶
            'AP': 1,   # 苹果 1元/吨
            'CJ': 1,   # 红枣 1元/吨
            'UR': 1,   # 尿素 1元/吨
            'JR': 1,   # 粳稻 1元/吨
            'LR': 1,   # 晚稻 1元/吨
            'RI': 1,   # 早稻 1元/吨
            'WH': 1,   # 强麦 1元/吨
            'PM': 1,   # 普麦 1元/吨
            'CY': 1,   # 棉纱 1元/吨
            'PF': 1,   # 短纤 1元/吨
            'PK': 1,   # 花生 1元/吨
            'WS': 1,   # 白麦 1元/吨
            'WT': 1,   # 小麦 1元/吨
            'SM': 2,   # 棉纱 2元/吨
            'LU': 1,   # 原油 1元/吨
        }
        return tick_sizes.get(variety, 1)


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
