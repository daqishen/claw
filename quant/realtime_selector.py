#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
当日实时选股 - 使用本地历史数据
直接扫描本地数据，筛选今日触发买点的股票
"""

import tushare as ts
import pandas as pd
import os
import time
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
MAX_DISTANCE_HIGH = 12
MIN_VOL_RATIO = 1.5

# 行业黑名单 (历史回测胜率显著低于平均水平的行业)
INDUSTRY_BLACKLIST = {'医药商业', '生物制药', '化学制药', '中成药'}

# 批量获取设置
BATCH_SIZE = 45  # 每批获取的股票数量


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
            holds = sum(1 for r in group['5%止盈止损结果'] if r == '持有')
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


def calculate_vol_ratio(rt_data, vol_20d_avg, pre_close):
    """
    计算量比
    收盘后直接用当前成交量，不需要预估
    注意：rt_k返回的vol单位是股，需要转换为手（除以100）
    """
    # rt_k返回的vol单位是股，需要转换为手（除以100）
    current_vol = rt_data['vol'] / 100
    
    # 直接用当前成交量计算量比
    vol_ratio = current_vol / vol_20d_avg
    
    return vol_ratio


def get_today_data_batch(ts_codes):
    """
    批量获取今日实时数据
    使用 pro.rt_k() 一次性获取多只股票的实时数据
    """
    if not ts_codes:
        return {}
    
    try:
        # 拼接股票代码，用逗号分隔
        ts_code_str = ','.join(ts_codes)
        df = pro.rt_k(ts_code=ts_code_str)
        
        if df is not None and len(df) > 0:
            result = {}
            for _, row in df.iterrows():
                code = row['ts_code']
                result[code] = {
                    'pre_close': row['pre_close'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'vol': row['vol'],
                    'amount': row['amount'],
                    'num': row.get('num', 0)
                }
            return result
    except Exception as e:
        pass
    
    return {}


def select_stocks_realtime():
    """使用本地历史数据筛选今日买点"""
    print("="*60)
    print("当日实时选股")
    print("="*60)
    
    today = datetime.now().strftime('%Y-%m-%d')
    today_str = datetime.now().strftime('%Y%m%d')
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
    
    # 剔除行业黑名单
    _stock_list_path = os.path.join(OUTPUT_DIR, 'a_stock_list.csv')
    if os.path.exists(_stock_list_path):
        _stock_list_df = pd.read_csv(_stock_list_path)
        _industry_map = dict(zip(_stock_list_df['ts_code'], _stock_list_df['industry']))
        all_stocks['industry'] = all_stocks['ts_code'].map(_industry_map)
        all_stocks = all_stocks[~all_stocks['industry'].isin(INDUSTRY_BLACKLIST)]
        print(f"  剔除行业黑名单后: {len(all_stocks)} 只")
    
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
    
    # ==================== 第一阶段：用本地历史数据筛选候选股票 ====================
    print("\n第一阶段：扫描本地数据，筛选候选股票...")
    
    candidate_stocks = []  # 候选股票列表
    
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
            
            # 用昨日数据分析（本地最新数据）
            yesterday = df.iloc[-1]  # 昨日K线
            
            # 15日涨幅（基于昨日收盘价）
            price_15d_ago = df.iloc[-16]['close']
            change_15d = (yesterday['close'] - price_15d_ago) / price_15d_ago * 100
            
            # 60日高点
            high_60d = df.iloc[-60:]['high'].max()
            distance_to_high = (high_60d - yesterday['close']) / high_60d * 100
            
            # 20日均量
            vol_20d_avg = df.iloc[-21:-1]['vol'].mean()
            
            # 昨日量比
            vol_ratio_yesterday = yesterday['vol'] / vol_20d_avg
            
            # 前两天缩量条件（与stock_selector.py一致：检查昨日和前天）
            vol_condition = (df.iloc[-1]['vol'] < vol_20d_avg) or (df.iloc[-2]['vol'] < vol_20d_avg)
            
            # 新增过滤条件：跌破5日新低 AND 低于昨日实体 → 不开仓
            # 条件1：昨日最低点跌破5日最低
            low_5day = df.iloc[-5:-1]['low'].min()
            is_broken_5day_low = yesterday['low'] <= low_5day * 1.001
            
            # 条件2：昨日收盘价低于昨日K线实体上沿
            yesterday_open = yesterday['open']
            yesterday_close = yesterday['close']
            yesterday_high = max(yesterday_open, yesterday_close)
            is_below_yesterday_body = yesterday['close'] < yesterday_high
            
            # 两个条件同时满足才过滤
            new_filter = is_broken_5day_low and is_below_yesterday_body
            
            # 第一阶段筛选：满足基础条件（涨幅、高点距离、缩量）+ 新过滤条件
            # 暂时不要求量比，因为今日量比需要实时数据
            if (change_15d < MAX_CHANGE_15D and 
                distance_to_high < MAX_DISTANCE_HIGH and 
                vol_condition and 
                not new_filter):
                
                candidate_stocks.append({
                    'full_code': full_code,
                    'code_short': code_short,
                    'yesterday_close': yesterday['close'],
                    'yesterday_vol': yesterday['vol'],
                    'yesterday_low': yesterday['low'],
                    'yesterday_open': yesterday['open'],
                    'vol_20d_avg': vol_20d_avg,
                    'change_15d': change_15d,
                    'distance_to_high': distance_to_high,
                    'price_15d_ago': price_15d_ago,
                    'high_60d': high_60d,
                    'low_5day': low_5day,
                })
        
        except Exception as e:
            continue
    
    print(f"第一阶段：找到 {len(candidate_stocks)} 只候选股票")
    
    # ==================== 第二阶段：批量获取实时数据并最终筛选 ====================
    print("\n第二阶段：批量获取实时数据...")
    
    results = []
    
    # 每次批量获取的股票数量
    BATCH_SIZE = 45
    
    for i in range(0, len(candidate_stocks), BATCH_SIZE):
        batch = candidate_stocks[i:i+BATCH_SIZE]
        batch_codes = [c['full_code'] for c in batch]
        
        print(f"  处理第 {i//BATCH_SIZE + 1} 批 ({len(batch_codes)} 只)...")
        
        # 批量获取实时数据
        rt_data = get_today_data_batch(batch_codes)
        
        for candidate in batch:
            full_code = candidate['full_code']
            
            if full_code not in rt_data:
                continue
            
            today_rt = rt_data[full_code]
            
            # 计算预估量比（考虑交易时间）
            vol_ratio = calculate_vol_ratio(today_rt, candidate['vol_20d_avg'], today_rt.get('pre_close', 0))
            
            # 重新计算15日涨幅（用实时收盘价）
            change_15d = (today_rt['close'] - candidate['price_15d_ago']) / candidate['price_15d_ago'] * 100
            
            # 重新计算距60日高点
            distance_to_high = (candidate['high_60d'] - today_rt['close']) / candidate['high_60d'] * 100
            
            # 新增过滤条件：当日最低点跌破5日最低 AND 收盘价低于昨日实体上沿 → 不开仓
            # 条件1：今日最低点跌破5日最低（用昨日5日低点，因为今日还没收盘）
            is_broken_5day_low = today_rt['low'] <= candidate['low_5day'] * 1.001
            
            # 条件2：今日收盘价低于昨日K线实体上沿
            yesterday_high = max(candidate['yesterday_open'], candidate['yesterday_close'])
            is_below_yesterday_body = today_rt['close'] < yesterday_high
            
            # 两个条件同时满足才过滤
            new_filter = is_broken_5day_low and is_below_yesterday_body
            
            # 最终买入条件判断
            if (change_15d < MAX_CHANGE_15D and 
                distance_to_high < MAX_DISTANCE_HIGH and 
                vol_ratio > MIN_VOL_RATIO and 
                not new_filter):
                
                # 获取历史统计
                hist = historical_stats.get(full_code, {})
                
                results.append({
                    'ts_code': full_code,
                    'name': '',
                    'date': today,
                    'close': round(today_rt['close'], 2),
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
        
        # 每批之间稍作延时，避免触发限流
        if i + BATCH_SIZE < len(candidate_stocks):
            time.sleep(2)
    
    # 获取名称
    if results:
        result_df = pd.DataFrame(results)
        
        # 批量获取股票名称
        codes = result_df['ts_code'].tolist()
        try:
            # 批量获取（每批50只）
            for i in range(0, len(codes), 50):
                batch_codes = codes[i:i+50]
                basic_df = pro.stock_basic(ts_code=','.join(batch_codes), fields='ts_code,name')
                if basic_df is not None and len(basic_df) > 0:
                    for _, row in basic_df.iterrows():
                        result_df.loc[result_df['ts_code'] == row['ts_code'], 'name'] = row['name']
        except Exception as e:
            print(f"  获取名称失败: {e}")
        
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
