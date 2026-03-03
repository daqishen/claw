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
DATA_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_futures/1min'


class RealtimeTrader:
    """实时期货交易类"""
    
    def __init__(self, config_path: str, test_mode: bool = False):
        self.config = self.load_config(config_path)
        self.test_mode = test_mode
        
        # 设置 tushare token
        token = self.config.get('tushare_token', '')
        if token:
            ts.set_token(token)
            self.pro = ts.pro_api(token)
        else:
            self.pro = None
        
        # 缓存
        self.last_signal = {}  # 缓存上一次的信号，避免重复打印
        
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
    
    def load_contract_data(self):
        """加载合约数据"""
        for contract in self.config.get('contracts', []):
            ts_code = contract['ts_code']
            name = contract.get('name', ts_code)
            
            # 尝试读取实时数据
            df = self.get_realtime_data(ts_code)
            
            if df is not None and len(df) > 0:
                self.contract_data[ts_code] = {
                    'data': df,
                    'name': name,
                    'contract': contract
                }
                print(f"✓ 加载合约: {ts_code} ({name}) - {len(df)} 条数据")
            else:
                # 如果没有实时数据，尝试加载历史数据
                filename = ts_code.replace('.', '_') + '_1min.csv'
                filepath = os.path.join(DATA_DIR, filename)
                
                if os.path.exists(filepath):
                    df = pd.read_csv(filepath, encoding='utf-8-sig')
                    # 统一列名
                    if 'trade_time' in df.columns:
                        df['time'] = df['trade_time']
                        # 按时间排序
                        df = df.sort_values('time').reset_index(drop=True)
                    # 使用全部历史数据用于测试
                    self.contract_data[ts_code] = {
                        'data': df,
                        'name': name,
                        'contract': contract,
                        'is_simulation': True
                    }
                    print(f"⚠ 加载合约(模拟): {ts_code} ({name}) - {len(df)} 条数据")
                else:
                    print(f"✗ 合约数据不存在: {ts_code}")
    
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
            
            df = self.get_realtime_data(ts_code)
            
            if df is not None and len(df) > 0:
                self.contract_data[ts_code] = {
                    'data': df,
                    'name': name,
                    'contract': contract
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
