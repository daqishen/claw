#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一字涨停板次日收益统计
- 剔除创业板(300/301)、科创板(688)、北交所(BJ)、ST
- 数据源: 本地历史日线CSV
"""

import pandas as pd
import os
import sys
from datetime import datetime

DATA_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_stocks'
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/result'

# 读取股票列表获取名称（用于过滤ST）
STOCK_LIST_FILE = '/Users/qiyue/Desktop/test/claw/quant/result/a_stock_list.csv'


def load_stock_names():
    """加载股票名称，用于ST过滤"""
    try:
        df = pd.read_csv(STOCK_LIST_FILE)
        return dict(zip(df['ts_code'], df['name']))
    except:
        return {}


def is_valid_stock(ts_code: str, name: str) -> bool:
    """剔除创业板、科创板、北交所、ST"""
    if not name:
        return True  # 没有名称信息时不过滤
    if 'ST' in name or '*ST' in name:
        return False
    if ts_code.startswith('300') or ts_code.startswith('301'):
        return False
    if ts_code.startswith('688'):
        return False
    if ts_code.endswith('.BJ'):
        return False
    return True


def ts_code_from_filename(filename: str) -> str:
    """从文件名转换为ts_code格式"""
    name = filename.replace('.csv', '')
    if name.startswith('sh'):
        return name[2:] + '.SH'
    elif name.startswith('sz'):
        return name[2:] + '.SZ'
    elif name.startswith('bj'):
        return name[2:] + '.BJ'
    return ''


def is_limit_up_yizi(row, pre_close):
    """
    判断是否为一字涨停板
    条件:
    1. 四价合一 (open == high == low == close)
    2. 涨幅 >= 9.8% (主板10%涨停，允许小误差)
    """
    o, h, l, c = row['open'], row['high'], row['low'], row['close']
    
    # 四价合一
    if not (o == h == l == c):
        return False
    
    # 涨幅判断
    if pre_close <= 0:
        return False
    change_pct = (c - pre_close) / pre_close * 100
    
    # 主板涨停 ~10%
    if change_pct >= 9.8:
        return True
    
    return False


def analyze_stock(filepath: str, ts_code: str) -> list:
    """分析单只股票的一字涨停板"""
    try:
        df = pd.read_csv(filepath)
        if df is None or len(df) < 10:
            return []
        
        df = df.sort_values('trade_date').reset_index(drop=True)
        
        results = []
        
        for i in range(1, len(df) - 1):  # 需要前一天和后一天
            pre_close = df.iloc[i - 1]['close']
            today = df.iloc[i]
            tomorrow = df.iloc[i + 1]
            
            if is_limit_up_yizi(today, pre_close):
                buy_price = today['close']
                
                # 次日收益统计
                next_open = tomorrow['open']
                next_high = tomorrow['high']
                next_low = tomorrow['low']
                next_close = tomorrow['close']
                
                next_open_ret = (next_open - buy_price) / buy_price * 100
                next_close_ret = (next_close - buy_price) / buy_price * 100
                next_high_ret = (next_high - buy_price) / buy_price * 100
                next_low_ret = (next_low - buy_price) / buy_price * 100
                
                # 判断次日是否继续涨停
                next_change = (next_close - buy_price) / buy_price * 100
                next_is_limit = next_change >= 9.8
                
                # 判断是否连续一字板（次日也四价合一涨停）
                next_is_yizi = is_limit_up_yizi(tomorrow, buy_price)
                
                # 当日成交量（一字板通常缩量）
                vol = today['vol']
                pre_vol = df.iloc[i - 1]['vol']
                vol_ratio = vol / pre_vol if pre_vol > 0 else 0
                
                # 判断是首板一字还是连板一字
                is_first_yizi = True
                if i >= 2:
                    prev_pre_close = df.iloc[i - 2]['close']
                    if is_limit_up_yizi(df.iloc[i - 1], prev_pre_close):
                        is_first_yizi = False
                
                results.append({
                    'ts_code': ts_code,
                    'date': today['trade_date'],
                    'close': buy_price,
                    'pre_close': pre_close,
                    'change_pct': (buy_price - pre_close) / pre_close * 100,
                    'vol': vol,
                    'vol_ratio': vol_ratio,
                    'is_first_yizi': is_first_yizi,
                    'next_open': next_open,
                    'next_close': next_close,
                    'next_high': next_high,
                    'next_low': next_low,
                    'next_open_ret': next_open_ret,
                    'next_close_ret': next_close_ret,
                    'next_high_ret': next_high_ret,
                    'next_low_ret': next_low_ret,
                    'next_is_limit': next_is_limit,
                    'next_is_yizi': next_is_yizi,
                })
        
        return results
    except Exception as e:
        return []


def main():
    print("=" * 70)
    print("一字涨停板次日收益统计")
    print("=" * 70)
    
    # 加载股票名称
    stock_names = load_stock_names()
    print(f"股票名称库: {len(stock_names)} 只")
    
    # 遍历所有文件
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"数据文件: {len(files)} 个")
    
    all_results = []
    processed = 0
    skipped = 0
    
    for f in files:
        ts_code = ts_code_from_filename(f)
        if not ts_code:
            continue
        
        # 过滤
        name = stock_names.get(ts_code, '')
        if not is_valid_stock(ts_code, name):
            skipped += 1
            continue
        
        filepath = os.path.join(DATA_DIR, f)
        results = analyze_stock(filepath, ts_code)
        all_results.extend(results)
        
        processed += 1
        if processed % 500 == 0:
            print(f"  已处理 {processed} 只股票, 找到 {len(all_results)} 个一字涨停板...")
    
    print(f"\n处理完成: {processed} 只股票, 跳过 {skipped} 只")
    print(f"共找到 {len(all_results)} 个一字涨停板")
    
    if not all_results:
        print("未找到一字涨停板数据!")
        return
    
    df = pd.DataFrame(all_results)
    
    # ===================== 总体统计 =====================
    total = len(df)
    
    print("\n" + "=" * 70)
    print("📊 总体统计")
    print("=" * 70)
    
    print(f"\n一字涨停板总数: {total} 个")
    print(f"涉及股票: {df['ts_code'].nunique()} 只")
    
    # 首板一字 vs 连板一字
    first_count = df['is_first_yizi'].sum()
    cont_count = total - first_count
    print(f"首板一字: {first_count} 个 ({first_count/total*100:.1f}%)")
    print(f"连板一字: {cont_count} 个 ({cont_count/total*100:.1f}%)")
    
    # ===================== 次日开盘统计 =====================
    print("\n" + "-" * 70)
    print("【次日开盘跳空】")
    print("-" * 70)
    
    high_open = (df['next_open_ret'] > 0).sum()
    flat_open = (df['next_open_ret'] == 0).sum()
    low_open = (df['next_open_ret'] < 0).sum()
    
    print(f"  高开: {high_open} 个 ({high_open/total*100:.1f}%)")
    print(f"  平开: {flat_open} 个 ({flat_open/total*100:.1f}%)")
    print(f"  低开: {low_open} 个 ({low_open/total*100:.1f}%)")
    print(f"  平均开盘涨幅: {df['next_open_ret'].mean():.2f}%")
    print(f"  中位数开盘涨幅: {df['next_open_ret'].median():.2f}%")
    
    # ===================== 次日收盘统计 =====================
    print("\n" + "-" * 70)
    print("【次日收盘收益】")
    print("-" * 70)
    
    positive = (df['next_close_ret'] > 0).sum()
    zero = (df['next_close_ret'] == 0).sum()
    negative = (df['next_close_ret'] < 0).sum()
    
    print(f"  收涨: {positive} 个 ({positive/total*100:.1f}%)")
    print(f"  收平: {zero} 个 ({zero/total*100:.1f}%)")
    print(f"  收跌: {negative} 个 ({negative/total*100:.1f}%)")
    print(f"  平均收益: {df['next_close_ret'].mean():.2f}%")
    print(f"  中位数收益: {df['next_close_ret'].median():.2f}%")
    print(f"  最大收益: {df['next_close_ret'].max():.2f}%")
    print(f"  最大亏损: {df['next_close_ret'].min():.2f}%")
    
    # 收益分布
    print(f"\n  收益分布:")
    bins = [(-100, -5), (-5, -3), (-3, 0), (0, 3), (3, 5), (5, 10), (10, 100)]
    for low, high in bins:
        count = ((df['next_close_ret'] > low) & (df['next_close_ret'] <= high)).sum()
        bar = '█' * int(count / total * 50)
        print(f"    {low:>6.0f}% ~ {high:>3.0f}%: {count:>5} 个 ({count/total*100:>5.1f}%) {bar}")
    
    # ===================== 次日盘中最高/最低 =====================
    print("\n" + "-" * 70)
    print("【次日盘中极值】")
    print("-" * 70)
    print(f"  盘中最高平均: {df['next_high_ret'].mean():.2f}%")
    print(f"  盘中最低平均: {df['next_low_ret'].mean():.2f}%")
    print(f"  盘中最高中位数: {df['next_high_ret'].median():.2f}%")
    print(f"  盘中最低中位数: {df['next_low_ret'].median():.2f}%")
    
    # ===================== 次日继续涨停 =====================
    print("\n" + "-" * 70)
    print("【次日继续涨停】")
    print("-" * 70)
    
    next_limit = df['next_is_limit'].sum()
    next_yizi = df['next_is_yizi'].sum()
    print(f"  次日涨停: {next_limit} 个 ({next_limit/total*100:.1f}%)")
    print(f"  次日一字涨停: {next_yizi} 个 ({next_yizi/total*100:.1f}%)")
    print(f"  次日开板(非涨停): {total - next_limit} 个 ({(total-next_limit)/total*100:.1f}%)")
    
    # ===================== 首板一字 vs 连板一字对比 =====================
    print("\n" + "-" * 70)
    print("【首板一字 vs 连板一字 次日收益对比】")
    print("-" * 70)
    
    first_df = df[df['is_first_yizi'] == True]
    cont_df = df[df['is_first_yizi'] == False]
    
    if len(first_df) > 0:
        print(f"\n  首板一字 ({len(first_df)} 个):")
        print(f"    次日平均收益: {first_df['next_close_ret'].mean():.2f}%")
        print(f"    次日中位数收益: {first_df['next_close_ret'].median():.2f}%")
        print(f"    次日收涨比例: {(first_df['next_close_ret']>0).sum()/len(first_df)*100:.1f}%")
        print(f"    次日继续涨停: {first_df['next_is_limit'].sum()/len(first_df)*100:.1f}%")
    
    if len(cont_df) > 0:
        print(f"\n  连板一字 ({len(cont_df)} 个):")
        print(f"    次日平均收益: {cont_df['next_close_ret'].mean():.2f}%")
        print(f"    次日中位数收益: {cont_df['next_close_ret'].median():.2f}%")
        print(f"    次日收涨比例: {(cont_df['next_close_ret']>0).sum()/len(cont_df)*100:.1f}%")
        print(f"    次日继续涨停: {cont_df['next_is_limit'].sum()/len(cont_df)*100:.1f}%")
    
    # ===================== 缩量 vs 放量一字板 =====================
    print("\n" + "-" * 70)
    print("【缩量 vs 放量一字板 次日收益对比】")
    print("-" * 70)
    
    # 缩量: 量比 < 0.5 (相对前一天)
    shrink_df = df[df['vol_ratio'] < 0.5]
    expand_df = df[df['vol_ratio'] >= 0.5]
    
    if len(shrink_df) > 0:
        print(f"\n  缩量一字板 (量比<0.5, {len(shrink_df)} 个):")
        print(f"    次日平均收益: {shrink_df['next_close_ret'].mean():.2f}%")
        print(f"    次日中位数收益: {shrink_df['next_close_ret'].median():.2f}%")
        print(f"    次日收涨比例: {(shrink_df['next_close_ret']>0).sum()/len(shrink_df)*100:.1f}%")
    
    if len(expand_df) > 0:
        print(f"\n  放量一字板 (量比>=0.5, {len(expand_df)} 个):")
        print(f"    次日平均收益: {expand_df['next_close_ret'].mean():.2f}%")
        print(f"    次日中位数收益: {expand_df['next_close_ret'].median():.2f}%")
        print(f"    次日收涨比例: {(expand_df['next_close_ret']>0).sum()/len(expand_df)*100:.1f}%")
    
    # ===================== 保存详细数据 =====================
    # 添加股票名称
    df['name'] = df['ts_code'].map(stock_names)
    
    save_cols = ['ts_code', 'name', 'date', 'close', 'change_pct', 'vol', 'vol_ratio',
                 'is_first_yizi', 'next_open_ret', 'next_close_ret', 'next_high_ret',
                 'next_low_ret', 'next_is_limit', 'next_is_yizi']
    
    output_file = os.path.join(OUTPUT_DIR, 'yizi_limit_up_analysis.csv')
    df[save_cols].to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n📁 详细数据已保存: {output_file}")


if __name__ == '__main__':
    main()
