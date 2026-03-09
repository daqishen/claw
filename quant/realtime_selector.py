#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
当日实时选股 - 使用本地历史数据
直接扫描本地数据，筛选今日触发买点的股票
"""

import tushare as ts
import pandas as pd
import os
from datetime import datetime

# 设置token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'
ts.set_token(TOKEN)
pro = ts.pro_api()

# 路径配置
DATA_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_stocks'
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/result'
OUTPUT_FILE = f'{OUTPUT_DIR}/realtime_buy_points_{datetime.now().strftime("%Y%m%d")}.csv'

# 选股条件
MIN_MARKET_CAP = 50
MAX_CHANGE_15D = 15
MAX_DISTANCE_HIGH = 8
MIN_VOL_RATIO = 1.5


def get_historical_stats():
    """从历史买点数据中获取每只股票的历史统计"""
    stats = {}
    
    # 读取详细历史数据
    detailed_file = f'{OUTPUT_DIR}/stock_buy_points_detailed.csv'
    if os.path.exists(detailed_file):
        df = pd.read_csv(detailed_file)
        
        for ts_code, group in df.groupby('股票代码'):
            wins = sum(1 for r in group['5%止盈止损结果'] if r == '止盈')
            losses = sum(1 for r in group['5%止盈止损结果'] if r == '止损')
            holds = sum(1 for r in group['5%止盈止损结果'] if r in ['划痕', '持有'])
            total = wins + losses + holds
            
            if total > 0:
                win_rate = wins / total * 100
                avg_return = group['21日最大收益(%)'].mean()
                
                stats[ts_code] = {
                    '历史买点次数': total,
                    '历史胜率(%)': round(win_rate, 1),
                    '历史盈利次数': wins,
                    '历史亏损次数': losses,
                    '历史持有次数': holds,
                    '历史平均收益(%)': round(avg_return, 1)
                }
    
    return stats


def select_stocks_realtime():
    """使用本地历史数据筛选今日买点"""
    print("="*60)
    print("当日实时选股")
    print("="*60)
    
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"今日: {today}")
    
    # 获取历史统计数据
    print("\n读取历史统计数据...")
    historical_stats = get_historical_stats()
    print(f"已获取 {len(historical_stats)} 只股票的历史统计")
    
    # 获取有效股票列表(剔除科创板、ST、市值<50亿)
    print("\n获取股票列表...")
    
    all_stocks = []
    df_sh = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,symbol,name')
    df_sz = pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,symbol,name')
    all_stocks = pd.concat([df_sh, df_sz], ignore_index=True)
    
    # 剔除科创板和ST
    all_stocks = all_stocks[~all_stocks['ts_code'].str.startswith('688')]
    all_stocks = all_stocks[~all_stocks['name'].str.contains(r'ST|\*ST', na=False, regex=True)]
    
    # 获取市值
    pm_df = pro.stk_premarket(trade_date='20260306')
    
    market_caps = []
    for _, row in all_stocks.iterrows():
        ts_code = row['ts_code']
        try:
            if ts_code.endswith('.SH'):
                fname = f"sh{ts_code.replace('.SH', '')}.csv"
            else:
                fname = f"sz{ts_code.replace('.SZ', '')}.csv"
            
            price_df = pd.read_csv(f"{DATA_DIR}/{fname}")
            latest_price = price_df.iloc[-1]['close']
            
            pm_row = pm_df[pm_df['ts_code'] == ts_code]
            if len(pm_row) > 0:
                total_share = pm_row.iloc[0]['total_share']
                if pd.notna(total_share):
                    market_cap = total_share * latest_price / 10000
                else:
                    market_cap = 0
            else:
                market_cap = 0
        except:
            market_cap = 0
        
        market_caps.append(market_cap)
    
    all_stocks['market_cap'] = market_caps
    all_stocks = all_stocks[all_stocks['market_cap'] >= MIN_MARKET_CAP]
    
    # 剔除北交所
    all_stocks = all_stocks[~all_stocks['ts_code'].str.endswith('.BJ')]
    
    valid_stocks = set()
    for code in all_stocks['ts_code'].tolist():
        valid_stocks.add(code.replace('.SH', '').replace('.SZ', ''))
    
    print(f"有效股票数量(市值>=50亿): {len(valid_stocks)}")
    
    # 扫描本地数据
    print("\n扫描本地数据...")
    
    results = []
    files = os.listdir(DATA_DIR)
    
    for f in files:
        if not f.endswith('.csv') or f.startswith('bj'):
            continue
        
        ts_code = f.replace('.csv', '')
        if ts_code.startswith('sh'):
            full_code = f'{ts_code.replace("sh", "")}.SH'
            code_short = ts_code.replace('sh', '')
        elif ts_code.startswith('sz'):
            full_code = f'{ts_code.replace("sz", "")}.SZ'
            code_short = ts_code.replace('sz', '')
        else:
            continue
        
        # 检查是否在有效列表
        if code_short not in valid_stocks:
            continue
        
        # 剔除创业板
        if code_short.startswith(('300', '301')):
            continue
        
        try:
            df = pd.read_csv(f"{DATA_DIR}/{f}")
            df = df.sort_values('trade_date')
            
            if len(df) < 60:
                continue
            
            # 获取今日数据
            today_data = df[df['trade_date'] == '2026-03-09']
            if len(today_data) == 0:
                continue
            
            today_row = today_data.iloc[0]
            
            # 15日涨幅
            price_15d_ago = df.iloc[-16]['close']
            change_15d = (today_row['close'] - price_15d_ago) / price_15d_ago * 100
            
            # 60日高点
            high_60d = df.iloc[-60:]['high'].max()
            distance_to_high = (high_60d - today_row['close']) / high_60d * 100
            
            # 20日均量
            vol_20d_avg = df.iloc[-21:-1]['vol'].mean()
            
            # 量比
            vol_ratio = today_row['vol'] / vol_20d_avg
            
            # 前两天缩量条件
            vol_condition = (df.iloc[-2]['vol'] < vol_20d_avg) or (df.iloc[-3]['vol'] < vol_20d_avg)
            
            # 买入条件
            if (change_15d < MAX_CHANGE_15D and 
                distance_to_high < MAX_DISTANCE_HIGH and 
                vol_ratio > MIN_VOL_RATIO and 
                vol_condition):
                
                # 获取历史统计
                hist = historical_stats.get(full_code, {})
                
                results.append({
                    'ts_code': full_code,
                    'name': '',
                    'date': '2026-03-09',
                    'close': round(today_row['close'], 2),
                    'change_15d': round(change_15d, 1),
                    'distance_to_high': round(distance_to_high, 1),
                    'vol_ratio': round(vol_ratio, 1),
                    '历史买点次数': hist.get('历史买点次数', 0),
                    '历史胜率(%)': hist.get('历史胜率(%)', '-'),
                    '历史盈利次数': hist.get('历史盈利次数', 0),
                    '历史亏损次数': hist.get('历史亏损次数', 0),
                    '历史持有次数': hist.get('历史持有次数', 0),
                    '历史平均收益(%)': hist.get('历史平均收益(%)', '-'),
                })
        
        except Exception as e:
            continue
    
    # 获取名称
    if results:
        result_df = pd.DataFrame(results)
        
        codes = result_df['ts_code'].tolist()
        for code in codes[:100]:
            try:
                basic = pro.stock_basic(ts_code=code, fields='ts_code,name')
                if len(basic) > 0:
                    name = basic.iloc[0]['name']
                    result_df.loc[result_df['ts_code'] == code, 'name'] = name
            except:
                pass
        
        result_df = result_df.sort_values('vol_ratio', ascending=False)
        
        # 先打印英文列名的结果
        print(f"\n找到 {len(result_df)} 只符合买点的股票")
        
        print("\n符合买点的股票:")
        for _, r in result_df.head(10).iterrows():
            win_rate = r['历史胜率(%)']
            print(f"  {r['ts_code']} {r['name']}: 现价={r['close']}, 涨幅={r['change_15d']}%, 距高点={r['distance_to_high']}%, 量比={r['vol_ratio']}x, 胜率={win_rate}")
        
        # 重命名列为中文
        result_df = result_df.rename(columns={
            'ts_code': '股票代码',
            'name': '股票名称',
            'date': '最新日期',
            'close': '最新价',
            'change_15d': '15日涨幅(%)',
            'distance_to_high': '60日距高(%)',
            'vol_ratio': '量比'
        })
        
        # 添加状态列
        result_df['状态'] = '✅ 今日已触发买点'
        
        # 重新排序列
        columns = ['股票代码', '股票名称', '最新日期', '最新价', '15日涨幅(%)', '60日距高(%)', '量比', 
                   '历史买点次数', '历史胜率(%)', '历史盈利次数', '历史亏损次数', '历史持有次数', '历史平均收益(%)', '状态']
        result_df = result_df[columns]
        
        result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n结果已保存到: {OUTPUT_FILE}")
    else:
        print("\n没有找到符合买点的股票")
    
    return results


if __name__ == '__main__':
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    select_stocks_realtime()
