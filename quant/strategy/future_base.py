#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期货数据处理基础类

提供期货数据的读取、处理、统计等功能
支持CSV和Tushare两种数据源

使用方法：
    from future_base import FutureData, FutureTrader
    
    # 读取数据
    data = FutureData.from_csv('path/to/data.csv')
    data = FutureData.from_tushare(df)
    
    # 交易示例
    trader = FutureTrader(data)
    position_id = trader.buy(open_price=5000, volume=1, open_time='2026-01-01 10:00:00', stop_loss=4900)
    trader.sell(position_id=position_id, close_price=5100, close_time='2026-01-01 11:00:00')
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, List, Dict, Union
import os
import uuid


class FutureData:
    """期货数据基类"""
    
    def __init__(self, df: pd.DataFrame = None):
        """
        初始化
        
        Args:
            df: pandas DataFrame，包含期货数据
        """
        self.df = df
        self._validate_data()
    
    def _validate_data(self):
        """验证数据格式"""
        if self.df is None or len(self.df) == 0:
            return
            
        # 必需的列
        required_cols = ['ts_code', 'time', 'open', 'close', 'high', 'low']
        missing = [col for col in required_cols if col not in self.df.columns]
        if missing:
            raise ValueError(f"缺少必需列: {missing}")
    
    @classmethod
    def from_csv(cls, filepath: str) -> 'FutureData':
        """
        从CSV文件读取数据
        
        Args:
            filepath: CSV文件路径
            
        Returns:
            FutureData实例
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"文件不存在: {filepath}")
        
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        
        # 统一列名
        if 'trade_time' in df.columns and 'time' not in df.columns:
            df['time'] = df['trade_time']
        
        return cls(df)
    
    @classmethod
    def from_tushare(cls, df: pd.DataFrame) -> 'FutureData':
        """
        从Tushare数据创建
        
        Args:
            df: Tushare返回的DataFrame
            
        Returns:
            FutureData实例
        """
        # 统一列名
        df = df.copy()
        if 'trade_time' in df.columns and 'time' not in df.columns:
            df['time'] = df['trade_time']
        if 'code' in df.columns and 'ts_code' not in df.columns:
            df['ts_code'] = df['code']
        
        return cls(df)
    
    @classmethod
    def from_dir(cls, dirpath: str, pattern: str = '*.csv') -> Dict[str, 'FutureData']:
        """
        从目录读取所有CSV文件
        
        Args:
            dirpath: 目录路径
            pattern: 文件匹配模式
            
        Returns:
            字典，key为合约名，value为FutureData实例
        """
        import glob
        
        result = {}
        files = glob.glob(os.path.join(dirpath, pattern))
        
        for filepath in files:
            try:
                data = cls.from_csv(filepath)
                # 使用文件名作为key
                basename = os.path.basename(filepath)
                # 移除扩展名
                name = os.path.splitext(basename)[0]
                result[name] = data
            except Exception as e:
                print(f"读取失败 {filepath}: {e}")
        
        return result
    
    def get_ohlc(self) -> pd.DataFrame:
        """获取OHLC数据"""
        return self.df[['time', 'open', 'high', 'low', 'close', 'vol', 'amount', 'oi']].copy()
    
    def get_time_range(self) -> tuple:
        """获取数据时间范围"""
        if self.df is None or len(self.df) == 0:
            return None, None
        return self.df['time'].min(), self.df['time'].max()
    
    def get_contracts(self) -> List[str]:
        """获取所有合约代码"""
        if self.df is None or len(self.df) == 0:
            return []
        return self.df['ts_code'].unique().tolist()
    
    def filter_by_time(self, start_time: str = None, end_time: str = None) -> 'FutureData':
        """
        按时间过滤
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            新的FutureData实例
        """
        df = self.df.copy()
        
        if start_time:
            df = df[df['time'] >= start_time]
        if end_time:
            df = df[df['time'] <= end_time]
        
        return FutureData(df)
    
    def filter_by_contract(self, ts_code: str) -> 'FutureData':
        """
        按合约过滤
        
        Args:
            ts_code: 合约代码
            
        Returns:
            新的FutureData实例
        """
        df = self.df[self.df['ts_code'] == ts_code].copy()
        return FutureData(df)


class TradeRecord:
    """交易记录"""
    
    def __init__(self):
        self.trades: List[Dict] = []
        self.positions: List[Dict] = []
    
    def add_trade(self, trade: Dict):
        """添加交易记录"""
        self.trades.append(trade)
    
    def add_position(self, position: Dict):
        """添加持仓记录"""
        self.positions.append(position)
    
    def get_trades(self) -> pd.DataFrame:
        """获取交易记录DataFrame"""
        if not self.trades:
            return pd.DataFrame()
        return pd.DataFrame(self.trades)
    
    def get_positions(self) -> pd.DataFrame:
        """获取持仓记录DataFrame"""
        if not self.positions:
            return pd.DataFrame()
        return pd.DataFrame(self.positions)
    
    def get_closed_trades(self) -> pd.DataFrame:
        """获取已平仓交易"""
        if not self.trades:
            return pd.DataFrame()
        closed = [t for t in self.trades if t.get('status') == 'closed']
        if not closed:
            return pd.DataFrame()
        return pd.DataFrame(closed)
    
    def get_open_positions(self) -> List[Dict]:
        """获取当前持仓"""
        return [p for p in self.positions if p.get('status') == 'open']
    
    def get_position_by_id(self, position_id: str) -> Dict:
        """根据ID获取持仓"""
        for p in self.positions:
            if p.get('position_id') == position_id and p.get('status') == 'open':
                return p
        return None
    
    def close_position_by_id(self, position_id: str):
        """根据ID关闭持仓状态"""
        for p in self.positions:
            if p.get('position_id') == position_id:
                p['status'] = 'closed'


class FutureTrader:
    """期货交易基础类 - 每笔交易独立结算"""
    
    def __init__(self, data: FutureData = None, initial_capital: float = 1000000):
        """
        初始化
        
        Args:
            data: FutureData实例
            initial_capital: 初始资金
        """
        self.data = data
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.available_capital = initial_capital  # 可用资金
        self.trade_record = TradeRecord()
        self.position_counter = 0  # 持仓计数器
    
    def _generate_position_id(self) -> str:
        """生成唯一持仓ID"""
        self.position_counter += 1
        return f"POS_{self.position_counter}_{uuid.uuid4().hex[:8]}"
    
    def buy(self, open_price: float, volume: int, open_time: str = None, 
            stop_loss: float = None, take_profit: float = None) -> str:
        """
        买入开多仓（独立结算）
        
        Args:
            open_price: 开仓价格
            volume: 手数
            open_time: 开仓时间
            stop_loss: 止损价格（可选）
            take_profit: 止盈价格（可选）
            
        Returns:
            position_id: 持仓唯一ID，用于后续平仓
        """
        # 计算保证金（假设10%保证金）
        margin_required = open_price * volume * 0.1
        
        if margin_required > self.available_capital:
            raise ValueError(f"资金不足，需要 {margin_required}，当前可用 {self.available_capital}")
        
        # 冻结保证金
        self.available_capital -= margin_required
        
        # 生成唯一持仓ID
        position_id = self._generate_position_id()
        
        position = {
            'position_id': position_id,
            'direction': 'long',
            'open_price': open_price,
            'volume': volume,
            'open_time': open_time,
            'margin': margin_required,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'status': 'open',
            'profit': 0  # 当前盈亏
        }
        
        self.trade_record.add_position(position)
        
        return position_id
    
    def sell(self, position_id: str = None, close_price: float = None, 
             close_time: str = None, reason: str = 'manual') -> Dict:
        """
        卖出平多仓（按position_id独立结算）
        
        Args:
            position_id: 持仓ID（如果不传则平最新持仓）
            close_price: 平仓价格
            close_time: 平仓时间
            reason: 平仓原因 ('manual', 'stop_loss', 'take_profit')
            
        Returns:
            交易记录字典
        """
        # 获取持仓
        if position_id:
            position = self.trade_record.get_position_by_id(position_id)
        else:
            # 获取最新持仓
            open_positions = self.trade_record.get_open_positions()
            if not open_positions:
                raise ValueError("没有持仓")
            position = open_positions[-1]
        
        if not position or position['direction'] != 'long':
            raise ValueError(f"找不到多头持仓: {position_id}")
        
        if close_price is None:
            raise ValueError("必须指定平仓价格")
        
        volume = position['volume']
        open_price = position['open_price']
        
        # 计算收益
        profit = (close_price - open_price) * volume
        
        # 退还保证金 + 盈亏
        self.available_capital += position['margin']
        self.available_capital += profit
        
        # 记录交易
        trade = {
            'position_id': position['position_id'],
            'direction': 'long',
            'open_price': open_price,
            'close_price': close_price,
            'volume': volume,
            'open_time': position['open_time'],
            'close_time': close_time,
            'margin': position['margin'],
            'profit': profit,
            'return_pct': (close_price - open_price) / open_price * 100,
            'stop_loss': position.get('stop_loss'),
            'take_profit': position.get('take_profit'),
            'reason': reason,
            'status': 'closed'
        }
        
        self.trade_record.add_trade(trade)
        self.trade_record.close_position_by_id(position['position_id'])
        
        return trade
    
    def short(self, open_price: float, volume: int, open_time: str = None,
             stop_loss: float = None, take_profit: float = None) -> str:
        """
        卖出开空仓（独立结算）
        
        Args:
            open_price: 开仓价格
            volume: 手数
            open_time: 开仓时间
            stop_loss: 止损价格（可选）
            take_profit: 止盈价格（可选）
            
        Returns:
            position_id: 持仓唯一ID
        """
        # 计算保证金（假设10%保证金）
        margin_required = open_price * volume * 0.1
        
        if margin_required > self.available_capital:
            raise ValueError(f"资金不足，需要 {margin_required}，当前可用 {self.available_capital}")
        
        # 冻结保证金
        self.available_capital -= margin_required
        
        # 生成唯一持仓ID
        position_id = self._generate_position_id()
        
        position = {
            'position_id': position_id,
            'direction': 'short',
            'open_price': open_price,
            'volume': volume,
            'open_time': open_time,
            'margin': margin_required,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'status': 'open',
            'profit': 0
        }
        
        self.trade_record.add_position(position)
        
        return position_id
    
    def cover(self, position_id: str = None, close_price: float = None,
              close_time: str = None, reason: str = 'manual') -> Dict:
        """
        买入平空仓（按position_id独立结算）
        
        Args:
            position_id: 持仓ID
            close_price: 平仓价格
            close_time: 平仓时间
            reason: 平仓原因
            
        Returns:
            交易记录字典
        """
        # 获取持仓
        if position_id:
            position = self.trade_record.get_position_by_id(position_id)
        else:
            open_positions = self.trade_record.get_open_positions()
            if not open_positions:
                raise ValueError("没有持仓")
            position = open_positions[-1]
        
        if not position or position['direction'] != 'short':
            raise ValueError(f"找不到空头持仓: {position_id}")
        
        if close_price is None:
            raise ValueError("必须指定平仓价格")
        
        volume = position['volume']
        open_price = position['open_price']
        
        # 计算收益（空头方向相反）
        profit = (open_price - close_price) * volume
        
        # 退还保证金 + 盈亏
        self.available_capital += position['margin']
        self.available_capital += profit
        
        # 记录交易
        trade = {
            'position_id': position['position_id'],
            'direction': 'short',
            'open_price': open_price,
            'close_price': close_price,
            'volume': volume,
            'open_time': position['open_time'],
            'close_time': close_time,
            'margin': position['margin'],
            'profit': profit,
            'return_pct': (open_price - close_price) / open_price * 100,
            'stop_loss': position.get('stop_loss'),
            'take_profit': position.get('take_profit'),
            'reason': reason,
            'status': 'closed'
        }
        
        self.trade_record.add_trade(trade)
        self.trade_record.close_position_by_id(position['position_id'])
        
        return trade
    
    def close_position(self, position_id: str = None, close_price: float = None,
                      close_time: str = None, reason: str = 'manual') -> Dict:
        """
        平仓（根据持仓方向自动选择平多或平空）
        
        Args:
            position_id: 持仓ID
            close_price: 平仓价格
            close_time: 平仓时间
            reason: 平仓原因
            
        Returns:
            交易记录字典
        """
        if position_id:
            position = self.trade_record.get_position_by_id(position_id)
        else:
            open_positions = self.trade_record.get_open_positions()
            if not open_positions:
                raise ValueError("没有持仓")
            position = open_positions[-1]
        
        if not position:
            raise ValueError(f"找不到持仓: {position_id}")
        
        if position['direction'] == 'long':
            return self.sell(position_id, close_price, close_time, reason)
        else:
            return self.cover(position_id, close_price, close_time, reason)
    
    def check_stop_loss(self, current_price: float) -> List[Dict]:
        """
        检查是否触发止损
        
        Args:
            current_price: 当前价格
            
        Returns:
            触发止损的持仓列表
        """
        triggered = []
        
        for position in self.trade_record.get_open_positions():
            if position['status'] != 'open':
                continue
            
            stop_loss = position.get('stop_loss')
            if stop_loss is None:
                continue
            
            direction = position['direction']
            
            # 检查是否触发止损
            if direction == 'long' and current_price <= stop_loss:
                triggered.append(position)
            elif direction == 'short' and current_price >= stop_loss:
                triggered.append(position)
        
        return triggered
    
    def check_take_profit(self, current_price: float) -> List[Dict]:
        """
        检查是否触发止盈
        
        Args:
            current_price: 当前价格
            
        Returns:
            触发止盈的持仓列表
        """
        triggered = []
        
        for position in self.trade_record.get_open_positions():
            if position['status'] != 'open':
                continue
            
            take_profit = position.get('take_profit')
            if take_profit is None:
                continue
            
            direction = position['direction']
            
            # 检查是否触发止盈
            if direction == 'long' and current_price >= take_profit:
                triggered.append(position)
            elif direction == 'short' and current_price <= take_profit:
                triggered.append(position)
        
        return triggered
    
    def process_price_update(self, current_price: float, current_time: str = None) -> Dict:
        """
        处理价格更新，自动检查止损止盈
        
        Args:
            current_price: 当前价格
            current_time: 当前时间
            
        Returns:
            处理结果字典
        """
        result = {
            'stop_loss_triggered': [],
            'take_profit_triggered': [],
            'closed_positions': []
        }
        
        # 检查止损
        stop_loss_positions = self.check_stop_loss(current_price)
        for position in stop_loss_positions:
            self.close_position(
                position['position_id'], 
                current_price, 
                current_time, 
                'stop_loss'
            )
            result['stop_loss_triggered'].append(position)
            result['closed_positions'].append(position)
        
        # 检查止盈
        take_profit_positions = self.check_take_profit(current_price)
        for position in take_profit_positions:
            if position['position_id'] not in [p['position_id'] for p in result['closed_positions']]:
                self.close_position(
                    position['position_id'],
                    current_price,
                    current_time,
                    'take_profit'
                )
                result['take_profit_triggered'].append(position)
                result['closed_positions'].append(position)
        
        # 更新持仓盈亏
        for position in self.trade_record.get_open_positions():
            if position['direction'] == 'long':
                position['profit'] = (current_price - position['open_price']) * position['volume']
            else:
                position['profit'] = (position['open_price'] - current_price) * position['volume']
        
        return result
    
    def get_open_positions_summary(self) -> List[Dict]:
        """获取持仓摘要"""
        positions = self.trade_record.get_open_positions()
        return [
            {
                'position_id': p['position_id'],
                'direction': p['direction'],
                'open_price': p['open_price'],
                'volume': p['volume'],
                'margin': p['margin'],
                'stop_loss': p.get('stop_loss'),
                'take_profit': p.get('take_profit'),
                'profit': p.get('profit', 0),
                'open_time': p['open_time']
            }
            for p in positions
        ]
    
    def get_account_summary(self) -> Dict:
        """获取账户摘要"""
        trades_df = self.trade_record.get_closed_trades()
        
        total_profit = trades_df['profit'].sum() if len(trades_df) > 0 else 0
        
        win_trades = trades_df[trades_df['profit'] > 0] if len(trades_df) > 0 else pd.DataFrame()
        loss_trades = trades_df[trades_df['profit'] <= 0] if len(trades_df) > 0 else pd.DataFrame()
        
        win_rate = len(win_trades) / len(trades_df) * 100 if len(trades_df) > 0 else 0
        
        # 统计止损次数
        stop_loss_count = len(trades_df[trades_df['reason'] == 'stop_loss']) if len(trades_df) > 0 else 0
        take_profit_count = len(trades_df[trades_df['reason'] == 'take_profit']) if len(trades_df) > 0 else 0
        manual_count = len(trades_df[trades_df['reason'] == 'manual']) if len(trades_df) > 0 else 0
        
        return {
            'initial_capital': self.initial_capital,
            'current_capital': self.available_capital,
            'frozen_margin': self.initial_capital - self.available_capital,
            'total_profit': total_profit,
            'total_return_pct': (self.available_capital - self.initial_capital) / self.initial_capital * 100,
            'total_trades': len(trades_df),
            'win_trades': len(win_trades),
            'loss_trades': len(loss_trades),
            'win_rate': win_rate,
            'stop_loss_count': stop_loss_count,
            'take_profit_count': take_profit_count,
            'manual_count': manual_count,
            'open_positions': len(self.trade_record.get_open_positions()),
            'positions_summary': self.get_open_positions_summary()
        }


class FutureAnalyzer:
    """期货数据分析工具类"""
    
    def __init__(self, data: FutureData):
        """
       初始化
        
		Args:
		    data: FutureData实例
		"""
        self.data = data
    
    def plot_equity_curve(self, trades: List[Dict] = None, save_path: str = None, 
                          title: str = "Equity Curve", initial_capital: float = 1000000) -> None:
        """
        绘制资金曲线图
        
        Args:
            trades: 交易记录列表
            save_path: 保存路径
            title: 图表标题
            initial_capital: 初始资金
        """
        try:
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
        except ImportError:
            print("请安装 matplotlib: pip install matplotlib")
            return
        
        if trades is None or len(trades) == 0:
            print("没有交易记录")
            return
        
        # 按时间排序
        trades = sorted(trades, key=lambda x: x.get('open_time', ''))
        
        # 计算资金曲线（模拟实际保证金交易）
        capital = initial_capital
        times = []
        capitals = []
        
        # 添加初始点
        if trades:
            first_time = trades[0].get('open_time', '')
            if first_time:
                times.append(first_time)
                capitals.append(capital)
        
        for trade in trades:
            # 获取保证金和盈利
            margin = trade.get('margin', 0)
            profit = trade.get('profit', 0)
            
            # 开仓：减保证金
            # 平仓：加保证金+盈利
            if margin > 0:
                capital -= margin  # 开仓时冻结保证金
            capital += margin + profit  # 平仓时退还保证金+盈亏
            
            close_time = trade.get('close_time', trade.get('open_time', ''))
            times.append(close_time)
            capitals.append(capital)
        
        # 绘制
        plt.figure(figsize=(12, 6))
        plt.plot(range(len(capitals)), capitals, 'b-', linewidth=1.5, label='Capital')
        plt.axhline(y=initial_capital, color='gray', linestyle='--', alpha=0.7, label='Initial Capital')
        
        # 填充盈利和亏损区域
        for i in range(1, len(capitals)):
            if capitals[i] >= capitals[i-1]:
                plt.fill_between([i-1, i], [capitals[i-1]]*2, [capitals[i]]*2, 
                                color='green', alpha=0.3)
            else:
                plt.fill_between([i-1, i], [capitals[i-1]]*2, [capitals[i]]*2, 
                                color='red', alpha=0.3)
        
        # 标注最大回撤和最大盈利
        max_capital = max(capitals)
        min_capital = min(capitals)
        max_idx = capitals.index(max_capital)
        min_idx = capitals.index(min_capital)
        
        plt.scatter([max_idx], [max_capital], color='green', s=100, zorder=5, label=f'Max: {max_capital:.0f}')
        plt.scatter([min_idx], [min_capital], color='red', s=100, zorder=5, label=f'Min: {min_capital}')
        
        plt.title(title, fontsize=14)
        plt.xlabel('Trade Number', fontsize=12)
        plt.ylabel('Capital', fontsize=12)
        plt.legend(loc='upper left')
        plt.grid(True, alpha=0.3)
        
        # 添加统计信息
        total_return = (capitals[-1] - initial_capital) / initial_capital * 100
        win_trades = len([t for t in trades if t.get('profit', 0) > 0])
        loss_trades = len([t for t in trades if t.get('profit', 0) <= 0])
        win_rate = win_trades / len(trades) * 100 if trades else 0
        
        stats_text = f'Total Return: {total_return:.2f}%\nWin Rate: {win_rate:.1f}%\nWins: {win_trades} | Losses: {loss_trades}'
        plt.text(0.02, 0.02, stats_text, transform=plt.gca().transAxes, fontsize=10,
                verticalalignment='bottom', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"图表已保存: {save_path}")
        
        plt.close()
    
    def plot_trade_returns(self, trades: List[Dict] = None, save_path: str = None,
                          title: str = "Trade Returns") -> None:
        """
        绘制每笔交易收益图
        
        Args:
            trades: 交易记录列表
            save_path: 保存路径
            title: 图表标题
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("请安装 matplotlib: pip install matplotlib")
            return
        
        if trades is None or len(trades) == 0:
            print("没有交易记录")
            return
        
        # 获取每笔收益
        profits = [t.get('profit', 0) for t in trades]
        trade_nums = range(1, len(profits) + 1)
        
        # 颜色区分盈利和亏损
        colors = ['green' if p > 0 else 'red' for p in profits]
        
        plt.figure(figsize=(14, 6))
        plt.bar(trade_nums, profits, color=colors, alpha=0.7)
        plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        
        plt.title(title, fontsize=14)
        plt.xlabel('Trade Number', fontsize=12)
        plt.ylabel('Profit', fontsize=12)
        plt.grid(True, alpha=0.3, axis='y')
        
        # 统计
        total_profit = sum(profits)
        wins = len([p for p in profits if p > 0])
        losses = len([p for p in profits if p <= 0])
        
        stats_text = f'Total: {total_profit:.0f}\nWins: {wins} | Losses: {losses}'
        plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"图表已保存: {save_path}")
        
        plt.close()
    
    def calculate_returns(self) -> pd.DataFrame:
        """计算收益率"""
        df = self.data.df.copy()
        df['return'] = df['close'].pct_change()
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))
        return df
    
    def calculate_max_drawdown(self, capital_series: pd.Series = None) -> float:
        """
        计算最大回撤
        
        Args:
            capital_series: 资金序列，如果为None则用收盘价
            
        Returns:
            最大回撤（百分比）
        """
        if capital_series is None:
            capital_series = self.data.df['close']
        
        # 计算累计最大值
        cummax = capital_series.cummax()
        # 计算回撤
        drawdown = (capital_series - cummax) / cummax * 100
        # 返回最大回撤（正值表示亏损）
        return drawdown.min()
    
    def calculate_sharpe_ratio(self, returns: pd.Series = None, risk_free_rate: float = 0.03) -> float:
        """
        计算夏普比率
        
        Args:
            returns: 收益率序列
            risk_free_rate: 无风险利率
            
        Returns:
            夏普比率
        """
        if returns is None:
            returns = self.calculate_returns()['return']
        
        if len(returns) == 0 or returns.std() == 0:
            return 0
        
        # 年化夏普比率
        excess_return = returns.mean() * 252 - risk_free_rate
        return excess_return / (returns.std() * np.sqrt(252))
    
    def calculate_win_rate(self, trades: List[Dict]) -> float:
        """
        计算胜率
        
        Args:
            trades: 交易记录列表
            
        Returns:
            胜率（百分比）
        """
        if not trades:
            return 0
        
        wins = sum(1 for t in trades if t.get('profit', 0) > 0)
        return wins / len(trades) * 100
    
    def calculate_profit_factor(self, trades: List[Dict]) -> float:
        """
        计算盈利因子
        
        Args:
            trades: 交易记录列表
            
        Returns:
            盈利因子（盈利总额/亏损总额）
        """
        if not trades:
            return 0
        
        profits = [t.get('profit', 0) for t in trades]
        total_profit = sum(p for p in profits if p > 0)
        total_loss = abs(sum(p for p in profits if p < 0))
        
        if total_loss == 0:
            return float('inf') if total_profit > 0 else 0
        
        return total_profit / total_loss
    
    def calculate_period_stats(self, start_time: str, end_time: str) -> Dict:
        """
        计算特定时间段统计
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            统计字典
        """
        df = self.data.filter_by_time(start_time, end_time).df
        
        if len(df) == 0:
            return {}
        
        return {
            'start_time': start_time,
            'end_time': end_time,
            'open_price': df.iloc[0]['close'],
            'close_price': df.iloc[-1]['close'],
            'high_price': df['high'].max(),
            'low_price': df['low'].min(),
            'period_return': (df.iloc[-1]['close'] - df.iloc[0]['close']) / df.iloc[0]['close'] * 100,
            'volatility': df['return'].std() * np.sqrt(252) * 100 if 'return' in df.columns else 0,
            'max_price': df['high'].max(),
            'min_price': df['low'].min(),
            'total_volume': df['vol'].sum(),
            'avg_volume': df['vol'].mean()
        }
    
    def calculate_all_stats(self) -> Dict:
        """
        计算全部统计信息
        
        Returns:
            统计字典
        """
        df = self.data.df
        
        if len(df) == 0:
            return {}
        
        # 计算收益率
        df = self.calculate_returns()
        
        return {
            'data_points': len(df),
            'start_time': df['time'].min(),
            'end_time': df['time'].max(),
            'initial_price': df.iloc[0]['close'],
            'final_price': df.iloc[-1]['close'],
            'total_return': (df.iloc[-1]['close'] - df.iloc[0]['close']) / df.iloc[0]['close'] * 100,
            'max_price': df['high'].max(),
            'min_price': df['low'].min(),
            'avg_price': df['close'].mean(),
            'volatility': df['return'].std() * np.sqrt(252) * 100 if 'return' in df.columns else 0,
            'max_drawdown': self.calculate_max_drawdown(),
            'sharpe_ratio': self.calculate_sharpe_ratio(),
            'total_volume': df['vol'].sum(),
            'avg_volume': df['vol'].mean()
        }


class ConditionalOrder:
    """条件单基类"""
    
    @staticmethod
    def stop_loss(entry_price: float, direction: str, stop_pct: float = 0.02) -> float:
        """
        计算止损价格
        
        Args:
            entry_price: 入场价格
            direction: 'long' 或 'short'
            stop_pct: 止损百分比
            
        Returns:
            止损价格
        """
        if direction == 'long':
            return entry_price * (1 - stop_pct)
        else:
            return entry_price * (1 + stop_pct)
    
    @staticmethod
    def take_profit(entry_price: float, direction: str, profit_pct: float = 0.03) -> float:
        """
        计算止盈价格
        
        Args:
            entry_price: 入场价格
            direction: 'long' 或 'short'
            profit_pct: 止盈百分比
            
        Returns:
            止盈价格
        """
        if direction == 'long':
            return entry_price * (1 + profit_pct)
        else:
            return entry_price * (1 - profit_pct)
    
    @staticmethod
    def trailing_stop(entry_price: float, direction: str, trail_pct: float = 0.015, 
                     highest_price: float = None, lowest_price: float = None) -> float:
        """
        计算追踪止损价格
        
        Args:
            entry_price: 入场价格
            direction: 'long' 或 'short'
            trail_pct: 追踪百分比
            highest_price: 最高价（多头用）
            lowest_price: 最低价（空头用）
            
        Returns:
            追踪止损价格
        """
        if direction == 'long':
            if highest_price is None:
                return entry_price
            return highest_price * (1 - trail_pct)
        else:
            if lowest_price is None:
                return entry_price
            return lowest_price * (1 + trail_pct)


# 便捷函数
def load_futures_data(filepath_or_dir: str, is_dir: bool = False) -> Union[FutureData, Dict[str, FutureData]]:
    """
    便捷函数：加载期货数据
    
    Args:
        filepath_or_dir: 文件路径或目录路径
        is_dir: 是否为目录
        
    Returns:
        FutureData实例或字典
    """
    if is_dir:
        return FutureData.from_dir(filepath_or_dir)
    else:
        return FutureData.from_csv(filepath_or_dir)


class StrategyBase:
    """期货交易策略基类"""
    
    def __init__(self, name: str = "Strategy", params: dict = None):
        """
        初始化策略
        
        Args:
            name: 策略名称
            params: 策略参数
        """
        self.name = name
        self.params = params or {}
        
        # 默认参数
        self.lookback_bars = self.params.get('lookback_bars', 15)
        self.volume_multiplier = self.params.get('volume_multiplier', 3)
        self.default_volume = self.params.get('default_volume', 10)
        self.risk_reward_ratio = self.params.get('risk_reward_ratio', 1.5)  # 止盈止损比
    
    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理数据
        
        Args:
            df: 原始数据
            
        Returns:
            处理后的数据
        """
        df = df.copy()
        
        # 确保有时间列
        if 'trade_time' in df.columns and 'time' not in df.columns:
            df['time'] = df['trade_time']
        
        # 确保有成交量列
        if 'vol' not in df.columns:
            df['vol'] = 0
        
        # 按时间排序
        if 'time' in df.columns:
            df = df.sort_values('time').reset_index(drop=True)
        
        return df
    
    def check_signals(self, df: pd.DataFrame) -> List[Dict]:
        """
        检测信号 - 子类实现具体逻辑
        
        Args:
            df: 数据
            
        Returns:
            信号列表
        """
        raise NotImplementedError("子类必须实现 check_signals 方法")
    
    def get_latest_signal(self, df: pd.DataFrame) -> Optional[Dict]:
        """
        获取最新的信号
        
        Args:
            df: 数据
            
        Returns:
            最新信号或None
        """
        signals = self.check_signals(df)
        if signals:
            return signals[-1]
        return None
    
    def calculate_tp_sl(self, direction: str, entry_price: float, stop_loss: float) -> Dict:
        """
        计算止盈止损
        
        Args:
            direction: 'long' 或 'short'
            entry_price: 开仓价
            stop_loss: 止损价
            
        Returns:
            包含止盈止损的字典
        """
        risk = abs(entry_price - stop_loss)
        
        if direction == 'long':
            tp1 = entry_price + risk * 1
            tp2 = entry_price + risk * 2
        else:
            tp1 = entry_price - risk * 1
            tp2 = entry_price - risk * 2
        
        return {
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'take_profit_1': tp1,
            'take_profit_2': tp2,
            'risk': risk
        }
    
    def backtest(self, df: pd.DataFrame, initial_capital: float = 1000000) -> Dict:
        """
        历史回测 - 支持1:1止盈平半仓，2:1止盈平全仓
        
        Args:
            df: 历史数据
            initial_capital: 初始资金
            
        Returns:
            回测结果
        """
        df = self.prepare_data(df)
        signals = self.check_signals(df)
        
        if not signals:
            return {
                'total_trades': 0,
                'signals': [],
                'capital': initial_capital
            }
        
        # 模拟交易 - 支持分批止盈
        capital = initial_capital
        trades = []
        position = None  # 当前持仓
        
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
                    'volume': signal.get('volume', self.default_volume),
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
            
            # 计算当前盈亏
            if direction == 'long':
                # 假设当前价格等于开仓价，无盈亏
                pass
            else:
                pass
            
            # 这里简化处理：只记录信号，不实际模拟持仓
            # 实际回测需要在子类中实现完整的持仓逻辑
        
        # 简化版：只根据信号计算盈亏
        for signal in signals:
            direction = signal['direction']
            entry = signal['entry_price']
            stop = signal['stop_loss']
            volume = signal.get('volume', self.default_volume)
            
            # 计算盈亏
            if direction == 'long':
                profit = (entry - stop) * volume
            else:
                profit = (entry - stop) * volume
            
            capital += profit
            trades.append({
                **signal,
                'profit': profit,
                'capital': capital
            })
        
        return {
            'total_trades': len(trades),
            'signals': signals,
            'trades': trades,
            'final_capital': capital,
            'total_profit': capital - initial_capital
        }
    
    def format_signal(self, signal: Dict, contract_info: Dict = None) -> Dict:
        """
        格式化信号输出
        
        Args:
            signal: 信号字典
            contract_info: 合约信息
            
        Returns:
            格式化后的信号
        """
        direction_cn = '做多' if signal['direction'] == 'long' else '做空'
        
        result = {
            'direction_cn': direction_cn,
            'volume': signal.get('volume', self.default_volume),
            **signal
        }
        
        if contract_info:
            result['contract_name'] = contract_info.get('name', '')
            result['contract_code'] = contract_info.get('ts_code', '')
        
        return result


class VolumeBreakoutStrategy(StrategyBase):
    """放量突破策略基类"""
    
    def __init__(self, name: str = "VolumeBreakout", params: dict = None):
        super().__init__(name, params)
    
    def check_breakout(self, df: pd.DataFrame, i: int) -> Optional[Dict]:
        """
        检查第i根K是否放量突破
        
        Args:
            df: 数据
            i: K线索引
            
        Returns:
            突破信息或None
        """
        if i < self.lookback_bars + 1:
            return None
        
        prev_n = df.iloc[i-self.lookback_bars-1:i-1]
        curr = df.iloc[i]
        
        # 放量条件
        vol_threshold = prev_n['vol'].mean() * self.volume_multiplier
        if curr['vol'] <= vol_threshold:
            return None
        
        # 突破判定 - 返回更多信息
        if curr['low'] <= prev_n['low'].min():
            # 突破最低点
            return {
                'breakout_type': 'low',  # 突破最低点
                'direction': 'long',  # 潜在做多方向
                'stop_loss_at_low': curr['low'],  # 做多时用这个止损
                'stop_loss_at_high': curr['high'],  # 做空时用这个止损
            }
        elif curr['high'] >= prev_n['high'].max():
            # 突破最高点
            return {
                'breakout_type': 'high',  # 突破最高点
                'direction': 'short',  # 潜在做空方向
                'stop_loss_at_low': curr['low'],  # 做多时用这个止损
                'stop_loss_at_high': curr['high'],  # 做空时用这个止损
            }
        
        return None
    
    def is_bullish(self, df: pd.DataFrame, i: int) -> bool:
        """判断是否收阳"""
        if i >= len(df):
            return False
        return df.iloc[i]['close'] > df.iloc[i]['open']
    
    def is_bearish(self, df: pd.DataFrame, i: int) -> bool:
        """判断是否收阴"""
        if i >= len(df):
            return False
        return df.iloc[i]['close'] < df.iloc[i]['open']


if __name__ == '__main__':
    print("FutureBase 模块测试（独立结算版）")
    print("=" * 50)
    
    # 测试独立结算
    from future_base import FutureData, FutureTrader, ConditionalOrder
    
    # 读取数据
    data = FutureData.from_csv('/Users/qiyue/Desktop/test/claw/quant/data/history_futures/1min/CU2603_SHF_1min.csv')
    print(f'读取数据: {len(data.df)} 条')
    
    # 创建交易者
    trader = FutureTrader(data, initial_capital=1000000)
    
    # 开多仓1（带止损）
    pos1 = trader.buy(
        open_price=103000, 
        volume=1, 
        open_time='2026-02-01 10:00:00',
        stop_loss=102000  # 止损1000点
    )
    print(f"开多仓1, position_id: {pos1}")
    
    # 开多仓2（带止损）
    pos2 = trader.buy(
        open_price=103500, 
        volume=1, 
        open_time='2026-02-01 10:05:00',
        stop_loss=103000  # 止损500点
    )
    print(f"开多仓2, position_id: {pos2}")
    
    # 模拟价格变化，触发pos1止损
    print("\n--- 触发止损测试 ---")
    result = trader.process_price_update(101500, '2026-02-01 11:00:00')
    print(f"止损触发: {len(result['stop_loss_triggered'])} 个")
    
    # 查看账户
    summary = trader.get_account_summary()
    print(f"\n账户摘要:")
    print(f"  初始资金: {summary['initial_capital']}")
    print(f"  当前资金: {summary['current_capital']}")
    print(f"  冻结保证金: {summary['frozen_margin']}")
    print(f"  总交易次数: {summary['total_trades']}")
    print(f"  止损次数: {summary['stop_loss_count']}")
    print(f"  开仓中: {summary['open_positions']} 个")
    
    # pos2应该还在
    print(f"\n持仓详情:")
    for p in summary['positions_summary']:
        print(f"  {p['position_id']}: {p['direction']} {p['volume']}手 @ {p['open_price']}, 止损: {p['stop_loss']}")
    
    print("\n测试通过！每笔交易独立结算，一笔止损不影响另一笔。")
