#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时期货交易策略脚本

功能：
1. 读取配置文件
2. 获取当日实时/日内数据
3. 使用策略算法判定开仓条件
4. 轮询执行，输出开仓信号

使用方法：
    python3 realtime_trading.py [--test] [--once]
    
    --test: 使用历史数据模拟（测试模式）
    --once: 只运行一次，不轮询
"""

import sys
import os
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# 添加策略路径
sys.path.insert(0, '/Users/qiyue/Desktop/test/claw/quant/strategy')

import tushare as ts
from future_base import FutureData

# 配置路径
CONFIG_FILE = '/Users/qiyue/Desktop/test/claw/quant/realtime_future_config.json'


class RealtimeTrader:
    """实时期货交易类"""
    
    def __init__(self, config_path: str, test_mode: bool = False):
        self.config = self.load_config(config_path)
        self.test_mode = test_mode
        
        # 获取配置的分钟级别
        self.time_frame = self.config.get('time_frame', '5min')  # 默认5分钟
        base_data_dir = self.config.get('data_dir', '/Users/qiyue/Desktop/test/claw/quant/data/history_futures')
        self.data_dir = os.path.join(base_data_dir, self.time_frame)
        
        print(f"📊 使用时间周期: {self.time_frame}")
        print(f"📁 数据目录: {self.data_dir}")
        
        # 设置 tushare token
        token = self.config.get('tushare_token', '')
        if token:
            ts.set_token(token)
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
        
        # 缓存
        self.last_signal = {}  # 缓存上一次的信号，避免重复打印
        self.reported_breakouts = set()  # 已提示的放量K集合，避免重复提示
        
        # 加载策略
        self.strategies = self.load_strategies()
        
        # 加载合约数据
        self.contract_data = {}
        self.load_contract_data()
    
    def load_config(self, path: str) -> dict:
        """加载配置文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def load_strategies(self) -> dict:
        """加载策略模块"""
        strategies = {}
        
        for strat_config in self.config.get('strategies', []):
            name = strat_config['name']
            module_name = strat_config['module']
            class_name = strat_config['class']
            params = strat_config.get('params', {})
            
            try:
                # 动态导入模块
                module = __import__(module_name, fromlist=[class_name])
                strategy_class = getattr(module, class_name)
                
                # 创建策略实例
                strategy_instance = strategy_class(params=params)
                
                strategies[name] = {
                    'instance': strategy_instance,
                    'params': params,
                    'config': strat_config
                }
                print(f"✓ 加载策略: {name}")
            except Exception as e:
                print(f"✗ 加载策略失败 {name}: {e}")
        
        return strategies
    
    def resample_1min_to_target(self, df_1min: pd.DataFrame, target_frame: str) -> pd.DataFrame:
        """
        将1分钟K线合并成目标周期K线
        
        Args:
            df_1min: 1分钟数据
            target_frame: 目标周期 (5min, 15min, 30min, 60min)
        
        Returns:
            合并后的数据
        """
        if df_1min is None or len(df_1min) == 0:
            return pd.DataFrame()
        
        df = df_1min.copy()
        
        # 确保有时间列
        if 'trade_time' in df.columns and 'time' not in df.columns:
            df['time'] = df['trade_time']
        
        if 'time' not in df.columns:
            return pd.DataFrame()
        
        # 解析时间并设置为索引
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        
        # 根据目标周期重采样
        # 提取周期数字
        frame_minutes = int(target_frame.replace('min', ''))
        
        # 重采样
        df_resampled = df[['open', 'high', 'low', 'close', 'vol']].resample(f'{frame_minutes}min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'vol': 'sum'
        })
        
        # 去除空值并重置索引
        df_resampled = df_resampled.dropna()
        df_resampled = df_resampled.reset_index()
        df_resampled = df_resampled.rename(columns={'time': 'trade_time'})
        
        return df_resampled
    
    def load_contract_data(self):
        """加载合约数据"""
        for contract in self.config.get('contracts', []):
            ts_code = contract['ts_code']
            name = contract.get('name', ts_code)
            
            if self.test_mode:
                # 测试模式：加载历史数据
                filename = ts_code.replace('.', '_') + f'_{self.time_frame}.csv'
                filepath = os.path.join(self.data_dir, filename)
                
                if os.path.exists(filepath):
                    df = pd.read_csv(filepath, encoding='utf-8-sig')
                    if 'trade_time' in df.columns:
                        df['time'] = df['trade_time']
                    df = df.sort_values('time').reset_index(drop=True)
                    
                    self.contract_data[ts_code] = {
                        'data': df,
                        'name': name,
                        'contract': contract,
                        'is_simulation': True
                    }
                    print(f"⚠ 加载合约({self.time_frame},历史): {ts_code} ({name}) - {len(df)} 条")
                else:
                    print(f"✗ 历史数据不存在: {ts_code}")
            else:
                # 实盘模式：获取当日1分钟数据并合并
                df_1min = self.get_realtime_data(ts_code)
                
                if df_1min is not None and len(df_1min) > 0:
                    # 合并成目标周期
                    if self.time_frame == '1min':
                        df = df_1min
                    else:
                        df = self.resample_1min_to_target(df_1min, self.time_frame)
                    
                    if len(df) > 0:
                        df['time'] = df.get('time') or df.get('trade_time', '')
                        
                        self.contract_data[ts_code] = {
                            'data': df,
                            'name': name,
                            'contract': contract,
                            'is_simulation': False
                        }
                        print(f"✓ 加载合约({self.time_frame},实时): {ts_code} ({name}) - {len(df)} 条")
                    else:
                        print(f"✗ 无有效数据: {ts_code}")
                else:
                    print(f"✗ 获取实时数据失败: {ts_code}")
    
    def get_realtime_data(self, ts_code: str) -> pd.DataFrame:
        """获取当日实时数据"""
        try:
            # 尝试使用 rt_fut_min_daily 接口
            df = self.pro.rt_fut_min_daily(ts_code=ts_code, freq='1MIN')
            
            if df is not None and len(df) > 0:
                # 统一列名
                df = df.rename(columns={
                    'code': 'ts_code',
                    'time': 'trade_time'
                })
                return df
        except Exception as e:
            print(f"获取实时数据失败 {ts_code}: {e}")
        
        return None
    
    def is_market_open(self) -> bool:
        """检查是否在交易时段"""
        now = datetime.now()
        current_time = now.time()
        
        # 从配置获取交易时段
        trading_config = self.config.get('trading', {})
        market_hours = trading_config.get('market_hours', 
            {
                "session1_start": "09:00",
                "session1_end": "10:15",
                "session2_start": "10:30",
                "session2_end": "11:30",
                "session3_start": "13:30",
                "session3_end": "15:00",
                "session4_start": "21:00",
                "session4_end": "23:00"
            })
        
        # 构建所有交易时段
        sessions = []
        for key in ['session1', 'session2', 'session3', 'session4']:
            start = market_hours.get(f"{key}_start")
            end = market_hours.get(f"{key}_end")
            if start and end:
                try:
                    sessions.append((
                        datetime.strptime(start, "%H:%M").time(),
                        datetime.strptime(end, "%H:%M").time()
                    ))
                except:
                    pass
        
        for start, end in sessions:
            if start <= current_time <= end:
                return True
        
        return False
    
    def check_strategy_signal(self, df: pd.DataFrame, strategy_config: dict) -> list:
        """
        检查策略信号 - 使用策略类的check_signals方法
        
        Args:
            df: 数据
            strategy_config: 策略配置
            
        Returns:
            所有信号列表
        """
        strategy_instance = strategy_config['instance']
        
        # 预处理数据
        if 'trade_time' in df.columns and 'time' not in df.columns:
            df = df.copy()
            df['time'] = df['trade_time']
        
        if 'time' in df.columns:
            df = df.sort_values('time').reset_index(drop=True)
        
        # 获取所有信号
        signals = strategy_instance.check_signals(df)
        
        # 确保每个信号有volume字段
        for signal in signals:
            if 'volume' not in signal:
                signal['volume'] = strategy_config['params'].get('default_volume', 10)
        
        return signals if signals else []
    
    def check_all_contracts(self) -> list:
        """检查所有合约的所有信号"""
        all_signals = []
        
        for ts_code, contract_info in self.contract_data.items():
            df = contract_info['data']
            name = contract_info['name']
            
            for strat_name, strategy_config in self.strategies.items():
                signals = self.check_strategy_signal(df, strategy_config)
                
                for signal in signals:
                    # 构建信号信息
                    signal_info = {
                        'contract': ts_code,
                        'name': name,
                        'strategy': strat_name,
                        'time': signal['time'],
                        'direction': signal['direction'],
                        'entry_price': signal['entry_price'],
                        'stop_loss': signal['stop_loss'],
                        'take_profit_1': signal.get('take_profit_1'),
                        'take_profit_2': signal.get('take_profit_2'),
                        'volume': signal.get('volume', 10),
                        'reason': signal.get('reason', ''),
                    }
                    
                    all_signals.append(signal_info)
        
        # 按时间排序
        all_signals.sort(key=lambda x: x['time'])
        
        return all_signals
    
    def check_volume_breakout(self, df: pd.DataFrame, strategy_config: dict) -> list:
        """检测放量K线（不判断是否开仓）"""
        signals = []
        
        # 直接使用已加载的策略实例
        strategy_instance = strategy_config.get('instance')
        if not strategy_instance:
            return []
        
        # 统一列名：确保有time列
        df = df.copy()
        if 'trade_time' in df.columns and 'time' not in df.columns:
            df['time'] = df['trade_time']
        
        if 'time' not in df.columns:
            return []
        
        try:
            lookback_bars = strategy_config['params'].get('lookback_bars', 15)
            volume_multiplier = strategy_config['params'].get('volume_multiplier', 3)
            
            if len(df) < lookback_bars + 2:
                return []
            
            # 检查所有未确认的放量K（检查所有K，不只最后10根）
            for i in range(lookback_bars + 1, len(df)):
                curr = df.iloc[i]
                prev_n = df.iloc[i-lookback_bars-1:i-1]
                
                # 放量条件
                vol_threshold = prev_n['vol'].mean() * volume_multiplier
                if curr['vol'] > vol_threshold:
                    # 放量了
                    if curr['low'] <= prev_n['low'].min():
                        direction = 'long'
                        breakout_type = 'low'
                    elif curr['high'] >= prev_n['high'].max():
                        direction = 'short'
                        breakout_type = 'high'
                    else:
                        continue
                    
                    # 获取时间字段
                    time_val = curr.get('time') or curr.get('trade_time', '')
                    
                    # 生成放量K的唯一key（用于去重）
                    breakout_key = f"{strategy_config['config'].get('ts_code', '')}_{time_val}"
                    
                    signals.append({
                        'time': time_val,
                        'direction': direction,
                        'price': curr['close'],
                        'volume': curr['vol'],
                        'avg_volume': prev_n['vol'].mean(),
                        'breakout_type': breakout_type,
                        'breakout_key': breakout_key,  # 用于去重
                    })
        
        except Exception as e:
            print(f"检测放量K出错: {e}")
        
        return signals
    
    def check_all_volume_breakouts(self) -> list:
        """检查所有合约的放量K"""
        all_breakouts = []
        
        for ts_code, contract_info in self.contract_data.items():
            df = contract_info['data']
            name = contract_info['name']
            
            for strat_name, strategy_config in self.strategies.items():
                breakouts = self.check_volume_breakout(df, strategy_config)
                
                for b in breakouts:
                    # 过滤掉已提示过的放量K
                    if b.get('breakout_key') in self.reported_breakouts:
                        continue
                    
                    all_breakouts.append({
                        'contract': ts_code,
                        'name': name,
                        'strategy': strat_name,
                        'time': b['time'],
                        'direction': b['direction'],
                        'price': b['price'],
                        'volume': b['volume'],
                        'avg_volume': b['avg_volume'],
                        'breakout_type': b['breakout_type'],
                        'breakout_key': b.get('breakout_key'),
                    })
        
        return all_breakouts
    
    def print_volume_breakout(self, b: dict):
        """打印放量K通报"""
        direction_cn = '🔴 放量下跌' if b['direction'] == 'short' else '🟢 放量上涨'
        breakout_cn = '突破最低点' if b['breakout_type'] == 'low' else '突破最高点'
        
        print(f"\n{'='*70}")
        print(f"🔥 放量K提示!")
        print(f"{'='*70}")
        print(f"📌 合约: {b['name']} ({b['contract']})")
        print(f"⏰ 时间: {b['time']}")
        print(f"📈 状态: {direction_cn} - {breakout_cn}")
        print(f"💰 价格: {b['price']}")
        print(f"📊 成交量: {b['volume']:.0f} (均量: {b['avg_volume']:.0f}, 倍数: {b['volume']/b['avg_volume']:.1f}x)")
        print(f"📋 等待后2根K确认方向后开仓...")
        print(f"{'='*70}\n")
    
    def print_signal(self, signal: dict, index: int = None):
        """打印信号"""
        # 转换numpy类型为Python原生类型
        def to_native(v):
            if hasattr(v, 'item'):  # numpy类型
                return v.item()
            return v
        
        direction_cn = '做多' if signal['direction'] == 'long' else '做空'
        breakout_type = '最低点' if signal['direction'] == 'long' else '最高点'
        
        header = f"\n{'='*70}"
        if index:
            header += f"\n📌 信号 #{index}"
        else:
            header += f"\n🔥 开仓信号触发!"
        
        print(header)
        print("="*70)
        print(f"📌 策略: {signal['strategy']}")
        print(f"📌 合约: {signal['name']} ({signal['contract']})")
        print(f"⏰ 时间: {signal['time']}")
        print(f"📈 方向: {direction_cn}")
        print(f"💰 开仓价: {to_native(signal['entry_price'])}")
        print(f"🛡️ 止损价: {to_native(signal['stop_loss'])}")
        print(f"🎯 止盈1: {to_native(signal['take_profit_1'])} (1:1)")
        print(f"🎯 止盈2: {to_native(signal['take_profit_2'])} (1:2)")
        print(f"📊 手数: {signal['volume']}")
        print(f"📝 原因: {signal.get('reason', '')}")
        print(f"📋 详情: 放量K线突破近15根K的{breakout_type}，后2根确认方向")
        print("="*70 + "\n")
    
    def refresh_realtime_data(self):
        """刷新实时数据"""
        for contract in self.config.get('contracts', []):
            ts_code = contract['ts_code']
            name = contract.get('name', ts_code)
            
            # 获取当日1分钟数据
            df_1min = self.get_realtime_data(ts_code)
            
            if df_1min is not None and len(df_1min) > 0:
                # 合并成目标周期
                if self.time_frame == '1min':
                    df = df_1min
                else:
                    df = self.resample_1min_to_target(df_1min, self.time_frame)
                
                if len(df) > 0:
                    df['time'] = df.get('time') or df.get('trade_time', '')
                    
                    self.contract_data[ts_code] = {
                        'data': df,
                        'name': name,
                        'contract': contract,
                        'is_simulation': False
                    }
    
    def run_once(self, first_run: bool = False):
        """
        执行一次检查
        
        Args:
            first_run: 是否是第一次运行（打印所有历史信号）
        """
        # 刷新实时数据
        if not self.test_mode:
            self.refresh_realtime_data()
        
        # 1. 先检查放量K并通报
        all_breakouts = self.check_all_volume_breakouts()
        
        if first_run:
            # 第一次运行，打印所有放量K
            for b in all_breakouts:
                self.print_volume_breakout(b)
                # 添加到已提示集合
                if b.get('breakout_key'):
                    self.reported_breakouts.add(b['breakout_key'])
        else:
            # 后续运行，只打印新出现的放量K
            for b in all_breakouts:
                # 添加到已提示集合（避免本次循环内重复）
                if b.get('breakout_key'):
                    self.reported_breakouts.add(b['breakout_key'])
                self.print_volume_breakout(b)
        
        # 2. 再检查开仓信号
        all_signals = self.check_all_contracts()
        
        if not all_signals:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ 无开仓信号")
            return
        
        if first_run:
            # 第一次运行，打印所有历史信号
            print(f"\n📊 今日共 {len(all_signals)} 个开仓信号:\n")
            for i, signal in enumerate(all_signals, 1):
                self.print_signal(signal, index=i)
        else:
            # 后续运行，只打印最新信号
            latest = all_signals[-1]  # 最新信号
            signal_key = f"{latest['contract']}_{latest['strategy']}_{latest['time']}"
            
            # 检查是否已经打印过
            if self.last_signal.get(latest['contract']) != signal_key:
                self.last_signal[latest['contract']] = signal_key
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🔔 新信号!")
                self.print_signal(latest, index=len(all_signals))
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ 无新信号")
    
    def run(self):
        """运行轮询"""
        polling_config = self.config.get('polling', {})
        interval = polling_config.get('interval_seconds', 30)
        
        print("\n" + "="*70)
        print("实时期货交易策略系统启动")
        print("="*70)
        print(f"📊 时间周期: {self.time_frame}")
        print(f"📁 数据目录: {self.data_dir}")
        
        # 第一次运行，打印所有历史信号
        self.run_once(first_run=True)
        
        if self.test_mode or not self.is_market_open():
            print(f"\n⚠️ 当前{'测试模式' if self.test_mode else '不在交易时段'}")
            print("只运行一次...")
            return
        
        print(f"\n🔄 开始轮询，间隔 {interval} 秒...")
        print("按 Ctrl+C 停止\n")
        
        while True:
            try:
                self.run_once(first_run=False)
                time.sleep(interval)
            except KeyboardInterrupt:
                print("\n\n停止轮询")
                break


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='实时期货交易策略')
    parser.add_argument('--config', '-c', default=CONFIG_FILE, help='配置文件路径')
    parser.add_argument('--test', '-t', action='store_true', help='测试模式（使用历史数据）')
    parser.add_argument('--once', '-o', action='store_true', help='只运行一次')
    
    args = parser.parse_args()
    
    # 创建交易器
    trader = RealtimeTrader(args.config, test_mode=args.test)
    
    if args.once:
        trader.run_once(first_run=True)
    else:
        trader.run()


if __name__ == '__main__':
    main()
