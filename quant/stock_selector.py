#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选股算法 - 关键买点筛选 + 卖点分析
条件:
1. 近15天涨幅 < 15%
2. 当前价格距离历史高点 < 5%
3. 昨日成交量 > 1.5倍 20日成交量均值
4. 剔除创业板 (300xxx, 301xxx)
5. 剔除ST
6. 剔除科创板 (688xxx)

卖点分析:
- 统计每一次买点后，未来一个月的最大收益率和最大回撤
- 找到获得最大收益的日期和最大回撤的日期
"""

import tushare as ts
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from stock_selector_config import stocks, stock_list_file

# 输出目录
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/result'
DATA_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_stocks'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 是否从本地读取数据 (True=本地, False=Tushare)
USE_LOCAL_DATA = True

# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 设置token
ts.set_token(TOKEN)
pro = ts.pro_api()

# 获取日期
end_date = datetime.now().strftime('%Y%m%d')
start_date_1y = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

# 最小市值门槛 (亿元)
MIN_MARKET_CAP = 50

# 缓存市值数据
market_cap_cache = {}
premarket_data = None


def get_premarket_data(trade_date: str = None) -> pd.DataFrame:
    """获取盘前股本数据"""
    global premarket_data
    if premarket_data is not None:
        return premarket_data
    
    try:
        # 获取最近一个有数据的交易日
        if trade_date is None:
            trade_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
        
        premarket_data = pro.stk_premarket(trade_date=trade_date)
        return premarket_data
    except Exception as e:
        print(f"获取盘前数据失败: {e}")
        return pd.DataFrame()


def get_market_cap(ts_code: str) -> float:
    """获取股票市值（亿元）"""
    if ts_code in market_cap_cache:
        return market_cap_cache[ts_code]
    
    try:
        # 尝试从本地数据获取最新收盘价
        if ts_code.endswith('.SH'):
            filename = f"sh{ts_code.replace('.SH', '')}.csv"
        elif ts_code.endswith('.SZ'):
            filename = f"sz{ts_code.replace('.SZ', '')}.csv"
        else:
            return 0
        
        file_path = f"{DATA_DIR}/{filename}"
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df = df.sort_values('trade_date')
            if len(df) > 0:
                latest_price = df.iloc[-1]['close']
                
                # 获取总股本 (使用stk_premarket)
                pm_df = get_premarket_data()
                if len(pm_df) > 0:
                    row = pm_df[pm_df['ts_code'] == ts_code]
                    if len(row) > 0:
                        total_share = row.iloc[0]['total_share']  # 总股本(万股)
                        if pd.notna(total_share):
                            market_cap = total_share * latest_price / 10000  # 亿元
                            market_cap_cache[ts_code] = market_cap
                            return market_cap
    except Exception as e:
        pass
    
    return 0


def is_valid_stock(ts_code: str, name: str) -> bool:
    """剔除创业板、ST、科创板、北交所、市值小于100亿"""
    if 'ST' in name or '*ST' in name:
        return False
    if ts_code.startswith('300') and ts_code.endswith('.SZ'):
        return False
    if ts_code.startswith('301') and ts_code.endswith('.SZ'):
        return False
    if ts_code.startswith('688') and ts_code.endswith('.SH'):
        return False
    # 剔除北交所
    if ts_code.endswith('.BJ'):
        return False
    
    # 检查市值
    market_cap = get_market_cap(ts_code)
    if market_cap > 0 and market_cap < MIN_MARKET_CAP:
        return False
    
    return True


def analyze_buy_point(df: pd.DataFrame, buy_idx: int) -> dict:
    """
    分析买点: 统计未来一个月/30天的最大收益率和最大回撤，以及5%止盈止损胜率
    
    Args:
        df: 股票数据
        buy_idx: 买点索引
    
    Returns:
        dict: 包含最大收益率、最大回撤和止盈止损胜率的信息
    """
    # 未来一个月(约20个交易日)和30天
    future_end_21 = min(buy_idx + 21, len(df))
    future_end_30 = min(buy_idx + 31, len(df))
    
    if future_end_21 <= buy_idx + 1:
        return None
    
    buy_price = df.iloc[buy_idx]['close']
    
    # 止盈止损参数
    tp_rate = 5.0  # 5%止盈
    sl_rate = -5.0  # -5%止损
    
    # 21天统计
    max_return_21 = -float('inf')
    max_return_date_21 = None
    max_return_days_21 = 0
    max_drawdown_21 = 0
    max_drawdown_date_21 = None
    max_drawdown_days_21 = 0
    
    # 30天统计
    max_return_30 = -float('inf')
    max_return_date_30 = None
    max_return_days_30 = 0
    max_drawdown_30 = 0
    max_drawdown_date_30 = None
    max_drawdown_days_30 = 0
    
    result = '持有'  # 结果: 止盈/止损/持有
    
    # 21天分析
    for j in range(buy_idx + 1, future_end_21):
        future_high = df.iloc[j]['high']
        future_low = df.iloc[j]['low']
        future_date = df.iloc[j]['trade_date']
        
        high_ret = (future_high - buy_price) / buy_price * 100
        low_ret = (future_low - buy_price) / buy_price * 100
        
        if high_ret >= tp_rate:
            result = '止盈'
        elif low_ret <= sl_rate:
            result = '止损'
        
        future_price = df.iloc[j]['close']
        ret = (future_price - buy_price) / buy_price * 100
        
        if ret > max_return_21:
            max_return_21 = ret
            max_return_date_21 = future_date
            max_return_days_21 = j - buy_idx
        
        peak = df.iloc[buy_idx:j+1]['high'].max()
        drawdown = (peak - future_price) / peak * 100
        
        if drawdown > max_drawdown_21:
            max_drawdown_21 = drawdown
            max_drawdown_date_21 = future_date
            max_drawdown_days_21 = j - buy_idx
    
    # 30天分析
    for j in range(buy_idx + 1, future_end_30):
        future_price = df.iloc[j]['close']
        future_date = df.iloc[j]['trade_date']
        ret = (future_price - buy_price) / buy_price * 100
        
        if ret > max_return_30:
            max_return_30 = ret
            max_return_date_30 = future_date
            max_return_days_30 = j - buy_idx
        
        peak = df.iloc[buy_idx:j+1]['high'].max()
        drawdown = (peak - future_price) / peak * 100
        
        if drawdown > max_drawdown_30:
            max_drawdown_30 = drawdown
            max_drawdown_date_30 = future_date
            max_drawdown_days_30 = j - buy_idx
    
    return {
        'max_return': max_return_21,
        'max_return_date': max_return_date_21,
        'max_return_days': max_return_days_21,
        'max_drawdown': max_drawdown_21,
        'max_drawdown_date': max_drawdown_date_21,
        'max_drawdown_days': max_drawdown_days_21,
        'max_return_30': max_return_30,
        'max_return_date_30': max_return_date_30,
        'max_return_days_30': max_return_days_30,
        'max_drawdown_30': max_drawdown_30,
        'max_drawdown_date_30': max_drawdown_date_30,
        'max_drawdown_days_30': max_drawdown_days_30,
        'result': result,
    }


def check_buy_points_all(df: pd.DataFrame) -> list:
    """
    遍历历史数据，找出所有满足买入条件的买点（不限制是否有未来数据）
    """
    results = []
    
    if len(df) < 25:
        return results
    
    # 遍历每一天（从有足够历史数据的那天开始）
    for i in range(25, len(df)):
        today = df.iloc[i]
        
        # 计算条件
        price_15d_ago = df.iloc[i-15]['close']
        change_15d = (today['close'] - price_15d_ago) / price_15d_ago * 100
        
        # 60日高点(到今天为止)
        high_60d = df.iloc[i-60:i+1]['high'].max()
        distance_to_high = (high_60d - today['close']) / high_60d * 100
        
        # 20日均量(到昨天为止)
        vol_20d_avg = df.iloc[i-20:i]['vol'].mean()
        vol_ratio = today['vol'] / vol_20d_avg
        
        # 新增条件：前两个交易日内，有一天的成交量低于20日均量
        vol_condition = (df.iloc[i-1]['vol'] < vol_20d_avg) or (df.iloc[i-2]['vol'] < vol_20d_avg)
        
        # 检查条件
        if change_15d < 15 and distance_to_high < 8 and vol_ratio > 1.5 and vol_condition:
            results.append({
                'date': today['trade_date'],
                'close': today['close'],
                'change_15d': change_15d,
                'high_60d': high_60d,
                'distance_to_high': distance_to_high,
                'industry': '',  # 行业 (后续填充)
                'is_hot_industry': False,  # 是否命中热点板块 (后续处理)
                'vol': today['vol'],
                'vol_avg': vol_20d_avg,
                'vol_ratio': vol_ratio,
            })
    
    return results


def check_buy_points(df: pd.DataFrame) -> list:
    """
    遍历历史数据，找出所有满足条件的买点，并分析卖点
    """
    results = []
    
    if len(df) < 30:
        return results
    
    # 遍历每一天
    for i in range(30, len(df) - 22):  # 需要有未来22天数据
        today = df.iloc[i]
        
        # 计算条件
        price_15d_ago = df.iloc[i-15]['close']
        change_15d = (today['close'] - price_15d_ago) / price_15d_ago * 100
        
        # 60日高点(到今天为止)
        high_60d = df.iloc[i-60:i+1]['high'].max()
        distance_to_high = (high_60d - today['close']) / high_60d * 100
        
        # 20日均量(到昨天为止)
        vol_20d_avg = df.iloc[i-20:i]['vol'].mean()
        vol_ratio = today['vol'] / vol_20d_avg
        
        # 新增条件：前两个交易日内，有一天的成交量低于20日均量
        vol_condition = (df.iloc[i-1]['vol'] < vol_20d_avg) or (df.iloc[i-2]['vol'] < vol_20d_avg)
        
        # 检查条件
        if change_15d < 15 and distance_to_high < 8 and vol_ratio > 1.5 and vol_condition:
            # 分析卖点
            sell_analysis = analyze_buy_point(df, i)
            
            results.append({
                'date': today['trade_date'],
                'close': today['close'],
                'change_15d': change_15d,
                'high_60d': high_60d,
                'distance_to_high': distance_to_high,
                'industry': '',  # 行业 (后续填充)
                'is_hot_industry': False,  # 是否命中热点板块 (后续处理)
                'vol': today['vol'],
                'vol_avg': vol_20d_avg,
                'vol_ratio': vol_ratio,
                **sell_analysis
            })
    
    return results


def analyze_stock(ts_code: str, name: str) -> list:
    """分析单只股票"""
    if not is_valid_stock(ts_code, name):
        return []
    
    try:
        # 从本地或Tushare读取数据
        if USE_LOCAL_DATA:
            # 转换为本地文件名格式
            if ts_code.endswith('.SH'):
                filename = f"sh{ts_code.replace('.SH', '')}.csv"
            elif ts_code.endswith('.SZ'):
                filename = f"sz{ts_code.replace('.SZ', '')}.csv"
            elif ts_code.endswith('.BJ'):
                filename = f"bj{ts_code.replace('.BJ', '')}.csv"
            else:
                return [], []
            
            filepath = os.path.join(DATA_DIR, filename)
            if not os.path.exists(filepath):
                return [], []
            
            df = pd.read_csv(filepath)
        else:
            df = pro.daily(ts_code=ts_code, start_date=start_date_1y, end_date=end_date)
        
        if df is None or len(df) < 50:  # 需要更多数据
            return [], []
        
        df = df.sort_values('trade_date')
        
        # 两种数据：
        # 1. 带卖点分析的买点（需要未来数据）
        buy_points_with_analysis = check_buy_points(df)
        # 2. 所有满足买入条件的买点（不需要未来数据）
        all_buy_points = check_buy_points_all(df)
        
        return buy_points_with_analysis, all_buy_points
    
    except Exception as e:
        return [], []


def main():
    print("="*80)
    print("选股分析 - 关键买点筛选 + 卖点分析")
    print("="*80)
    
    # 读取配置的股票列表
    stock_list = []
    
    if stocks:
        for ts_code in stocks:
            try:
                df_info = pro.stock_basic(ts_code=ts_code, fields='ts_code,name')
                if df_info is not None and len(df_info) > 0:
                    name = df_info.iloc[0]['name']
                    stock_list.append((ts_code, name))
            except:
                pass
        print(f"\n使用配置股票列表: {len(stock_list)} 只")
    elif stock_list_file:
        df_stocks = pd.read_csv(stock_list_file)
        for _, row in df_stocks.iterrows():
            ts_code = row['ts_code']
            name = row['name']
            if is_valid_stock(ts_code, name):
                stock_list.append((ts_code, name))
        print(f"\n使用股票列表文件: {len(stock_list)} 只")
    
    if not stock_list:
        print("未配置股票，请检查配置文件!")
        return
    
    # 分析每只股票
    print(f"开始分析...\n")
    
    all_results = []  # 带卖点分析的买点
    all_results_simple = []  # 所有满足买入条件的买点
    
    for i, (ts_code, name) in enumerate(stock_list):
        buy_points_with_analysis, all_buy_points = analyze_stock(ts_code, name)
        
        # 记录带卖点分析的买点
        if buy_points_with_analysis:
            print(f"[{i+1}/{len(stock_list)}] {name} ({ts_code}): 找到 {len(buy_points_with_analysis)} 个买点(有卖点分析)")
            for bp in buy_points_with_analysis:
                print(f"    买点: {bp['date']} | 买入价:{bp['close']:.2f}")
                print(f"         15日涨幅:{bp['change_15d']:.1f}% | 距60日高点:{bp['distance_to_high']:.1f}% | 量比:{bp['vol_ratio']:.1f}x")
                print(f"         21日: 最高收益 {bp['max_return']:.1f}% ({bp['max_return_days']}天,{bp.get('max_return_date', 'N/A')}) | 最大回撤 {bp['max_drawdown']:.1f}% ({bp['max_drawdown_days']}天,{bp.get('max_drawdown_date', 'N/A')})")
                print(f"         30日: 最高收益 {bp.get('max_return_30', 0):.1f}% ({bp.get('max_return_days_30', 0)}天,{bp.get('max_return_date_30', 'N/A')}) | 最大回撤 {bp.get('max_drawdown_30', 0):.1f}% ({bp.get('max_drawdown_days_30', 0)}天,{bp.get('max_drawdown_date_30', 'N/A')})")
                print(f"         5%止盈止损: {bp.get('result', '持有')}")
            all_results.append({
                'ts_code': ts_code,
                'name': name,
                'buy_points': buy_points_with_analysis
            })
        else:
            print(f"[{i+1}/{len(stock_list)}] {name} ({ts_code}): 无买点(有卖点分析)")
        
        # 记录所有满足买入条件的买点
        if all_buy_points:
            all_results_simple.append({
                'ts_code': ts_code,
                'name': name,
                'buy_points': all_buy_points
            })
    
    # 汇总
    print("\n" + "="*80)
    print("汇总")
    print("="*80)
    
    total_buy_points = sum(len(r['buy_points']) for r in all_results)
    print(f"分析的股票: {len(stock_list)} 只")
    print(f"有买点的股票: {len(all_results)} 只")
    print(f"买点总数: {total_buy_points} 个")
    
    # 统计最佳卖点
    if all_results:
        print("\n" + "-"*80)
        print("最佳卖点统计")
        print("-"*80)
        
        best_return_points = []
        worst_drawdown_points = []
        
        for r in all_results:
            for bp in r['buy_points']:
                best_return_points.append({
                    'name': r['name'],
                    'date': bp['date'],
                    'buy_price': bp['close'],
                    'max_return': bp['max_return'],
                    'max_return_date': bp['max_return_date'],
                })
                worst_drawdown_points.append({
                    'name': r['name'],
                    'date': bp['date'],
                    'buy_price': bp['close'],
                    'max_drawdown': bp['max_drawdown'],
                    'max_drawdown_date': bp['max_drawdown_date'],
                })
        
        # 按最大收益排序
        best_return_points.sort(key=lambda x: x['max_return'], reverse=True)
        print("\n【买点后未来一月收益最高 Top 5】")
        for j, p in enumerate(best_return_points[:5]):
            print(f"  {j+1}. {p['name']} | 买点:{p['date']} 买入价:{p['buy_price']:.2f} | 最高收益:{p['max_return']:.1f}% ({p['max_return_date']})")
        
        # 按最大回撤排序(回撤越小越好)
        worst_drawdown_points.sort(key=lambda x: x['max_drawdown'], reverse=True)
        print("\n【买点后未来一月回撤最大 Top 5】(应避开)")
        for j, p in enumerate(worst_drawdown_points[:5]):
            print(f"  {j+1}. {p['name']} | 买点:{p['date']} 买入价:{p['buy_price']:.2f} | 最大回撤:{p['max_drawdown']:.1f}% ({p['max_drawdown_date']})")
        
        # 计算总体止盈止损胜率
        all_results_list = []
        for r in all_results:
            for bp in r['buy_points']:
                all_results_list.append(bp.get('result', '持有'))
        
        total_bp = len(all_results_list)
        tp_total = all_results_list.count('止盈')
        sl_total = all_results_list.count('止损')
        hold_total = all_results_list.count('持有')
        win_rate_total = tp_total / total_bp * 100 if total_bp > 0 else 0
        
        print("\n【5%止盈止损统计】")
        print(f"  总买点: {total_bp} 个")
        print(f"  止盈: {tp_total} 个 ({tp_total/total_bp*100:.1f}%)")
        print(f"  止损: {sl_total} 个 ({sl_total/total_bp*100:.1f}%)")
        print(f"  持有: {hold_total} 个 ({hold_total/total_bp*100:.1f}%)")
        print(f"  胜率(止盈/(止盈+止损)): {tp_total/(tp_total+sl_total)*100:.1f}%")
    
    # 保存结果
    if all_results:
        # 1. 保存详细买点数据
        detailed_data = []
        for r in all_results:
            for bp in r['buy_points']:
                detailed_data.append({
                    '股票代码': r['ts_code'],
                    '股票名称': r['name'],
                    '买点日期': bp['date'],
                    '买入价': bp['close'],
                    '15日涨幅(%)': bp['change_15d'],
                    '60日距高(%)': bp['distance_to_high'],
                    '量比': bp['vol_ratio'],
                    '21日最大收益(%)': round(bp['max_return'], 2),
                    '21日收益日期': bp['max_return_date'],
                    '21日收益天数': bp.get('max_return_days', 0),
                    '21日最大回撤(%)': round(bp['max_drawdown'], 2),
                    '21日回撤日期': bp['max_drawdown_date'],
                    '21日回撤天数': bp.get('max_drawdown_days', 0),
                    '30日最大收益(%)': round(bp.get('max_return_30', 0), 2),
                    '30日收益日期': bp.get('max_return_date_30', ''),
                    '30日收益天数': bp.get('max_return_days_30', 0),
                    '30日最大回撤(%)': round(bp.get('max_drawdown_30', 0), 2),
                    '30日回撤日期': bp.get('max_drawdown_date_30', ''),
                    '30日回撤天数': bp.get('max_drawdown_days_30', 0),
                    '5%止盈止损结果': bp.get('result', '持有'),  # 止盈/止损/持有
                })
        
        df_detailed = pd.DataFrame(detailed_data)
        detailed_file = os.path.join(OUTPUT_DIR, 'stock_buy_points_detailed.csv')
        df_detailed.to_csv(detailed_file, index=False, encoding='utf-8-sig')
        print(f"\n📁 详细数据已保存: {detailed_file}")
        
        # 2. 保存汇总数据
        summary_data = []
        for r in all_results:
            bp_returns = [bp['max_return'] for bp in r['buy_points']]
            bp_drawdowns = [bp['max_drawdown'] for bp in r['buy_points']]
            
            # 计算10%止盈止损胜率
            results = [bp.get('result', '持有') for bp in r['buy_points']]
            tp_count = results.count('止盈')
            sl_count = results.count('止损')
            hold_count = results.count('持有')
            total = len(results)
            win_rate = tp_count / total * 100 if total > 0 else 0
            
            summary_data.append({
                '股票代码': r['ts_code'],
                '股票名称': r['name'],
                '买点数量': len(r['buy_points']),
                '平均最大收益(%)': round(sum(bp_returns) / len(bp_returns), 2),
                '最佳收益(%)': round(max(bp_returns), 2),
                '最差收益(%)': round(min(bp_returns), 2),
                '平均回撤(%)': round(sum(bp_drawdowns) / len(bp_drawdowns), 2),
                '最大回撤(%)': round(max(bp_drawdowns), 2),
                '止盈次数': tp_count,
                '止损次数': sl_count,
                '持有次数': hold_count,
                '胜率(%)': round(win_rate, 1),
            })
        
        df_summary = pd.DataFrame(summary_data)
        summary_file = os.path.join(OUTPUT_DIR, 'stock_buy_points_summary.csv')
        df_summary.to_csv(summary_file, index=False, encoding='utf-8-sig')
        print(f"📁 汇总数据已保存: {summary_file}")
    
    # ==================== 保存所有满足买入条件的买点（不含卖点分析）====================
    if all_results_simple:
        # 1. 保存所有满足买入条件的买点
        all_simple_data = []
        for r in all_results_simple:
            for bp in r['buy_points']:
                all_simple_data.append({
                    '股票代码': r['ts_code'],
                    '股票名称': r['name'],
                    '买点日期': bp['date'],
                    '买入价': round(bp['close'], 2),
                    '15日涨幅(%)': round(bp['change_15d'], 2),
                    '60日距高(%)': round(bp['distance_to_high'], 2),
                    '量比': round(bp['vol_ratio'], 2),
                })
        
        df_simple = pd.DataFrame(all_simple_data)
        simple_file = os.path.join(OUTPUT_DIR, 'stock_all_buy_points_simple.csv')
        df_simple.to_csv(simple_file, index=False, encoding='utf-8-sig')
        print(f"📁 所有买点(不含卖点分析)已保存: {simple_file}")
        print(f"   共 {len(all_simple_data)} 个买点")
        
        # 2. 获取最新数据日期
        latest_trading_date = None
        sample_files = []
        for f in os.listdir(DATA_DIR):
            if f.endswith('.csv') and f.startswith(('sh', 'sz')):
                sample_files.append(f)
                if len(sample_files) >= 100:
                    break
        
        for f in sample_files:
            try:
                df_temp = pd.read_csv(os.path.join(DATA_DIR, f))
                if 'trade_date' in df_temp.columns:
                    latest_file_date = df_temp['trade_date'].max()
                    if not latest_trading_date or latest_file_date > latest_trading_date:
                        latest_trading_date = latest_file_date
            except:
                pass
        
        # 3. 保存近期买点（距离最新开盘日30天内的买点，基于所有满足买入条件的买点）
        # 同时关联历史回测数据中的胜率信息
        if latest_trading_date:
            recent_cutoff = (datetime.strptime(latest_trading_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
            recent_buy_points = [bp for bp in all_simple_data if bp['买点日期'] >= recent_cutoff]
            
            if recent_buy_points:
                # 计算每只股票的历史回测统计
                stock_stats = {}
                if all_results:
                    for r in all_results:
                        ts_code = r['ts_code']
                        results = [bp.get('result', '持有') for bp in r['buy_points']]
                        tp = results.count('止盈')
                        sl = results.count('止损')
                        hold = results.count('持有')
                        total = len(results)
                        win_rate = tp / (tp + sl) * 100 if (tp + sl) > 0 else 0
                        
                        # 计算平均收益
                        returns = [bp['max_return'] for bp in r['buy_points']]
                        avg_return = sum(returns) / len(returns) if returns else 0
                        
                        stock_stats[ts_code] = {
                            '历史买点次数': total,
                            '历史胜率(%)': round(win_rate, 1),
                            '历史盈利次数': tp,
                            '历史亏损次数': sl,
                            '历史持有次数': hold,
                            '历史平均收益(%)': round(avg_return, 1)
                        }
                
                # 为近期买点添加历史统计
                for bp in recent_buy_points:
                    ts_code = bp['股票代码']
                    if ts_code in stock_stats:
                        bp.update(stock_stats[ts_code])
                    else:
                        bp['历史买点次数'] = 0
                        bp['历史胜率(%)'] = '-'
                        bp['历史盈利次数'] = 0
                        bp['历史亏损次数'] = 0
                        bp['历史持有次数'] = 0
                        bp['历史平均收益(%)'] = '-'
                
                df_recent = pd.DataFrame(recent_buy_points)
                recent_file = os.path.join(OUTPUT_DIR, 'stock_recent_buy_points.csv')
                df_recent.to_csv(recent_file, index=False, encoding='utf-8-sig')
                print(f"📁 近期买点({latest_trading_date}前30天)已保存: {recent_file}")
                print(f"   共 {len(recent_buy_points)} 个近期买点")
                
                # 打印近期买点的统计汇总
                if stock_stats:
                    stocks_with_recent = set(bp['股票代码'] for bp in recent_buy_points)
                    stats_summary = []
                    for ts_code in stocks_with_recent:
                        if ts_code in stock_stats:
                            stats_summary.append(stock_stats[ts_code])
                    
                    if stats_summary:
                        total_tp = sum(s['历史盈利次数'] for s in stats_summary)
                        total_sl = sum(s['历史亏损次数'] for s in stats_summary)
                        total_bp = total_tp + total_sl
                        overall_win_rate = total_tp / total_bp * 100 if total_bp > 0 else 0
                        print(f"   涉及股票 {len(stocks_with_recent)} 只")
                        print(f"   历史汇总: 盈利 {total_tp} 次, 亏损 {total_sl} 次, 胜率 {overall_win_rate:.1f}%")
            else:
                print(f"📁 近期买点: 无 (最新数据日期: {latest_trading_date}, 30天前: {recent_cutoff})")
    
    # ==================== 保存带卖点分析的买点（需要未来数据）====================
    
    # 保存结果 - 带卖点分析
    if all_results:
        # 保存所有带卖点分析的买点
        all_buy_points_data = []
        for r in all_results:
            for bp in r['buy_points']:
                all_buy_points_data.append({
                    '股票代码': r['ts_code'],
                    '股票名称': r['name'],
                    '买点日期': bp['date'],
                    '买入价': bp['close'],
                    '15日涨幅(%)': round(bp['change_15d'], 2),
                    '60日距高(%)': round(bp['distance_to_high'], 2),
                    '量比': round(bp['vol_ratio'], 2),
                    '21日最大收益(%)': round(bp['max_return'], 2),
                    '21日收益天数': bp.get('max_return_days', 0),
                    '21日最大回撤(%)': round(bp['max_drawdown'], 2),
                    '30日最大收益(%)': round(bp.get('max_return_30', 0), 2),
                    '30日最大回撤(%)': round(bp.get('max_drawdown_30', 0), 2),
                    '5%止盈止损结果': bp.get('result', '持有'),
                })
        
        df_all = pd.DataFrame(all_buy_points_data)
        all_points_file = os.path.join(OUTPUT_DIR, 'stock_all_buy_points.csv')
        df_all.to_csv(all_points_file, index=False, encoding='utf-8-sig')
        print(f"📁 所有买点(带卖点分析)已保存: {all_points_file}")
        
        # 保存明日买点预警（基于最新数据日期的下一个交易日）
        tomorrow_points = []
        for r in all_results:
            if not r['buy_points']:
                continue
            
            # 获取该股票的最新数据日期
            ts_code = r['ts_code']
            try:
                if ts_code.endswith('.SH'):
                    filename = f"sh{ts_code.replace('.SH', '')}.csv"
                elif ts_code.endswith('.SZ'):
                    filename = f"sz{ts_code.replace('.SZ', '')}.csv"
                else:
                    continue
                
                filepath = os.path.join(DATA_DIR, filename)
                if not os.path.exists(filepath):
                    continue
                
                df_stock = pd.read_csv(filepath)
                df_stock = df_stock.sort_values('trade_date')
                
                if len(df_stock) < 61:
                    continue
                
                # 检查最后一天是否符合买点条件
                last_idx = len(df_stock) - 1
                today = df_stock.iloc[last_idx]
                
                price_15d_ago = df_stock.iloc[last_idx - 15]['close']
                change_15d = (today['close'] - price_15d_ago) / price_15d_ago * 100
                
                high_60d = df_stock.iloc[last_idx - 60:last_idx + 1]['high'].max()
                distance_to_high = (high_60d - today['close']) / high_60d * 100
                
                vol_20d_avg = df_stock.iloc[last_idx - 20:last_idx]['vol'].mean()
                vol_ratio = today['vol'] / vol_20d_avg
                
                vol_condition = (df_stock.iloc[last_idx - 1]['vol'] < vol_20d_avg) or (df_stock.iloc[last_idx - 2]['vol'] < vol_20d_avg)
                
                if change_15d < 15 and distance_to_high < 8 and vol_ratio > 1.5 and vol_condition:
                    tomorrow_points.append({
                        '股票代码': ts_code,
                        '股票名称': r['name'],
                        '最新日期': today['trade_date'],
                        '最新价': today['close'],
                        '15日涨幅(%)': round(change_15d, 2),
                        '60日距高(%)': round(distance_to_high, 2),
                        '量比': round(vol_ratio, 2),
                        '状态': '✅ 今日已触发买点'
                    })
            except Exception as e:
                pass
        
        if tomorrow_points:
            df_tomorrow = pd.DataFrame(tomorrow_points)
            tomorrow_file = os.path.join(OUTPUT_DIR, 'stock_today_signals.csv')
            df_tomorrow.to_csv(tomorrow_file, index=False, encoding='utf-8-sig')
            print(f"📁 今日买点信号已保存: {tomorrow_file}")
            print(f"   共 {len(tomorrow_points)} 只股票今日触发买点")


if __name__ == '__main__':
    main()

# ============================================================
# 热点板块判断功能 (添加于 2026-03-06)
# ============================================================

# 热点板块缓存
hot_industry_cache = {}


def get_top10_industry(buy_date: str) -> list:
    """获取买点日期前3天的热门板块Top10
    
    Args:
        buy_date: 买点日期 (格式: YYYY-MM-DD)
    
    Returns:
        list: 热门板块名称列表
    """
    global hot_industry_cache
    
    # 转换日期格式
    date_str = buy_date.replace('-', '')
    
    # 检查缓存
    if date_str in hot_industry_cache:
        return hot_industry_cache[date_str]
    
    # 计算前3天的日期
    dt = datetime.strptime(buy_date, '%Y-%m-%d')
    trade_dates = []
    
    for i in range(1, 4):
        d = dt - timedelta(days=i)
        d_str = d.strftime('%Y%m%d')
        try:
            df = pro.moneyflow(trade_date=d_str)
            if len(df) > 0:
                trade_dates.append(d_str)
        except:
            pass
    
    if not trade_dates:
        return []
    
    # 汇总资金流
    all_flow = pd.DataFrame()
    for d in trade_dates:
        try:
            df = pro.moneyflow(trade_date=d)
            if len(df) > 0:
                all_flow = pd.concat([all_flow, df])
        except:
            pass
    
    if len(all_flow) == 0:
        return []
    
    # 获取行业信息
    stock_list = all_flow['ts_code'].unique().tolist()
    ind_list = []
    for i in range(0, min(len(stock_list), 300), 100):
        b = pro.stock_basic(ts_code=','.join(stock_list[i:i+100]), fields='ts_code,industry')
        ind_list.append(b)
    
    if not ind_list:
        return []
    
    ind_df = pd.concat(ind_list)
    all_flow = all_flow.merge(ind_df, on='ts_code')
    
    # 按行业汇总资金流
    industry_flow = all_flow.groupby('industry')['net_mf_amount'].sum()
    top10 = industry_flow.sort_values(ascending=False).head(10).index.tolist()
    
    # 缓存结果
    hot_industry_cache[date_str] = top10
    
    return top10


def check_industry_hot(buy_date: str, industry: str) -> bool:
    """判断股票行业是否在买点当日热点板块
    
    Args:
        buy_date: 买点日期
        industry: 股票行业
    
    Returns:
        bool: 是否在热点板块
    """
    if not industry:
        return False
    
    top10 = get_top10_industry(buy_date)
    return industry in top10

