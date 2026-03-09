#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
当日实时选股
- 使用tushare API获取股票列表
- 剔除市值<50亿、科创板、北交所、ST
- 使用东财接口获取实时行情
"""

import tushare as ts
import pandas as pd
import os
import requests
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
MIN_MARKET_CAP = 50  # 最小市值(亿)
MAX_CHANGE_15D = 15  # 15日涨幅<15%
MAX_DISTANCE_HIGH = 8  # 距高点<8%
MIN_VOL_RATIO = 1.5  # 量比>1.5


def get_a_stock_list_with_market_cap():
    """获取A股列表并获取市值，剔除不合格股票"""
    print("获取A股列表...")
    
    all_stocks = []
    
    # 沪市
    df_sh = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,symbol,name')
    if df_sh is not None:
        all_stocks.append(df_sh)
        print(f"  沪市: {len(df_sh)} 只")
    
    # 深市
    df_sz = pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,symbol,name')
    if df_sz is not None:
        all_stocks.append(df_sz)
        print(f"  深市: {len(df_sz)} 只")
    
    if not all_stocks:
        print("获取股票列表失败!")
        return None
    
    df = pd.concat(all_stocks, ignore_index=True)
    
    # 剔除科创板 (688开头)
    df = df[~df['ts_code'].str.startswith('688')]
    
    # 剔除ST股票
    df = df[~df['name'].str.contains('ST|\*ST', na=False, regex=True)]
    
    print(f"剔除科创板和ST后: {len(df)} 只")
    
    # 获取市值数据
    print("获取市值数据...")
    today = datetime.now().strftime('%Y%m%d')
    
    try:
        # 尝试获取当日premarket数据
        pm_df = pro.stk_premarket(trade_date=today)
        if pm_df is None or len(pm_df) == 0:
            # 如果没有当日数据，获取最近可用日期
            for i in range(1, 10):
                prev_date = (datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d')
                pm_df = pro.stk_premarket(trade_date=prev_date)
                if pm_df is not None and len(pm_df) > 0:
                    print(f"  使用 {prev_date} 的市值数据")
                    break
        
        if pm_df is not None and len(pm_df) > 0:
            # 合并市值
            df = df.merge(pm_df[['ts_code', 'total_share']], on='ts_code', how='left')
            
            # 获取最新价格计算市值
            market_caps = []
            for _, row in df.iterrows():
                ts_code = row['ts_code']
                try:
                    if ts_code.endswith('.SH'):
                        fname = f"sh{ts_code.replace('.SH', '')}.csv"
                    else:
                        fname = f"sz{ts_code.replace('.SZ', '')}.csv"
                    
                    price_df = pd.read_csv(f"{DATA_DIR}/{fname}")
                    latest_price = price_df.iloc[-1]['close']
                    total_share = row['total_share']
                    
                    if pd.notna(total_share) and total_share > 0:
                        market_cap = total_share * latest_price / 10000
                    else:
                        market_cap = 0
                except:
                    market_cap = 0
                
                market_caps.append(market_cap)
            
            df['market_cap'] = market_caps
            
            # 剔除市值<50亿
            df = df[df['market_cap'] >= MIN_MARKET_CAP]
            print(f"剔除市值<50亿后: {len(df)} 只")
        else:
            print("  无法获取市值数据，跳过市值过滤")
    
    except Exception as e:
        print(f"  获取市值失败: {e}")
    
    # 剔除北交所
    df = df[~df['ts_code'].str.endswith('.BJ')]
    
    print(f"最终股票数量: {len(df)} 只")
    return df


def get_realtime_data(stock_df):
    """获取实时行情"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_stocks = []
    
    markets = [
        ('m:0+t:6', '沪市主板'),
        ('m:1+t:2', '深市主板'),
    ]
    
    for fs, name in markets:
        for page in range(1, 30):
            url = f'https://push2.eastmoney.com/api/qt/clist/get?pn={page}&pz=200&po=1&np=1&fltt=2&invt=2&fid=f3&fs={fs}&fields=f2,f3,f4,f5,f6,f7,f12,f14,f15,f17'
            
            try:
                resp = requests.get(url, headers=headers, timeout=8)
                data = resp.json()
                
                if data.get('data') and data['data'].get('diff'):
                    stocks = data['data']['diff']
                    stocks = [s for s in stocks if 'ST' not in str(s.get('f14', ''))]
                    all_stocks.extend(stocks)
                    
                    if len(stocks) < 200:
                        break
                else:
                    break
            except Exception as e:
                break
            time.sleep(0.1)
    
    if not all_stocks:
        return None
    
    df = pd.DataFrame(all_stocks)
    df = df.drop_duplicates(subset=['f12'])
    df = df.rename(columns={
        'f2': 'close', 'f3': 'pct_chg', 'f4': 'change',
        'f5': 'vol', 'f12': 'ts_code', 'f14': 'name'
    })
    df = df[df['close'].notna() & (df['close'] > 0)]
    
    print(f"实时数据获取: {len(df)} 只")
    return df


def check_trade_date():
    """检查今日是否交易"""
    today = datetime.now().strftime('%Y%m%d')
    try:
        cal = pro.trade_cal(exchange='SSE', start_date=today, end_date=today)
        if len(cal) > 0 and cal.iloc[0]['is_open'] == 1:
            return True
    except:
        pass
    return False


def select_stocks_realtime():
    """实时选股主函数"""
    print("="*60)
    print("当日实时选股")
    print("="*60)
    
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"今日: {today}")
    print(f"是否交易日: {check_trade_date()}")
    
    # 获取过滤后的股票列表
    stock_df = get_a_stock_list_with_market_cap()
    
    if stock_df is None or len(stock_df) == 0:
        print("获取股票列表失败，使用历史数据模拟")
        return select_from_history()
    
    # 获取实时数据
    print("\n获取实时行情...")
    realtime_df = get_realtime_data(stock_df)
    
    if realtime_df is None or len(realtime_df) < 50:
        print("获取实时数据失败，使用历史数据模拟")
        return select_from_history()
    
    # 创建一个set来快速查找已过滤的股票(去掉交易所后缀)
    valid_stocks = set()
    for code in stock_df['ts_code'].tolist():
        valid_stocks.add(code.replace('.SH', '').replace('.SZ', ''))
    print(f"有效股票数量(市值>=50亿): {len(valid_stocks)}")
    
    # 选股
    results = []
    
    for _, row in realtime_df.iterrows():
        ts_code = row['ts_code']
        
        # 检查是否在有效股票列表中(去掉后缀匹配)
        ts_code_short = ts_code.replace('.SH', '').replace('.SZ', '')
        if ts_code_short not in valid_stocks:
            continue
        ts_code = row['ts_code']
        name = row['name']
        current_price = row['close']
        current_vol = row.get('vol', 0)
        
        if not ts_code or pd.isna(current_price) or current_price <= 0:
            continue
        
        # 剔除创业板
        if ts_code.startswith(('300', '301')):
            continue
        
        # 获取历史数据
        try:
            if ts_code.endswith('.SH'):
                fname = f"sh{ts_code.replace('.SH', '')}.csv"
            else:
                fname = f"sz{ts_code.replace('.SZ', '')}.csv"
            
            hist_df = pd.read_csv(f"{DATA_DIR}/{fname}")
            hist_df = hist_df.sort_values('trade_date')
            
            if len(hist_df) < 60:
                continue
            
            yesterday = hist_df.iloc[-1]
            price_15d_ago = hist_df.iloc[-16]['close']
            
            # 15日涨幅
            change_15d = (current_price - price_15d_ago) / price_15d_ago * 100
            
            # 60日高点
            high_60d = hist_df.iloc[-60:]['high'].max()
            distance_to_high = (high_60d - current_price) / high_60d * 100
            
            # 20日均量
            vol_20d_avg = hist_df.iloc[-21:-1]['vol'].mean()
            
            # 量比
            if pd.isna(current_vol) or current_vol == 0:
                current_vol = yesterday['vol']
            
            vol_ratio = current_vol / vol_20d_avg if vol_20d_avg > 0 else 0
            
            # 前两天缩量条件
            if len(hist_df) >= 3:
                vol_condition = (hist_df.iloc[-2]['vol'] < vol_20d_avg) or (hist_df.iloc[-3]['vol'] < vol_20d_avg)
            else:
                vol_condition = True
            
            # 买入条件
            if (change_15d < MAX_CHANGE_15D and 
                distance_to_high < MAX_DISTANCE_HIGH and 
                vol_ratio > MIN_VOL_RATIO and 
                vol_condition):
                
                results.append({
                    'ts_code': ts_code,
                    'name': name,
                    'date': today,
                    'close': round(current_price, 2),
                    'change_15d': round(change_15d, 1),
                    'distance_to_high': round(distance_to_high, 1),
                    'vol_ratio': round(vol_ratio, 1),
                })
        
        except Exception as e:
            continue
    
    # 保存结果
    if results:
        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values('vol_ratio', ascending=False)
        result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n找到 {len(results)} 只符合买点的股票")
        print(f"结果已保存到: {OUTPUT_FILE}")
        
        print("\n符合买点的股票:")
        for _, r in result_df.iterrows():
            print(f"  {r['ts_code']} {r['name']}: 现价={r['close']}, 涨幅={r['change_15d']}%, 距高点={r['distance_to_high']}%, 量比={r['vol_ratio']}x")
    else:
        print("\n没有找到符合买点的股票")
    
    return results


def select_from_history():
    """从历史数据选股"""
    recent_file = f'{OUTPUT_DIR}/stock_recent_buy_points.csv'
    if os.path.exists(recent_file):
        df = pd.read_csv(recent_file)
        latest_date = df['买点日期'].max()
        df_today = df[df['买点日期'] == latest_date]
        
        print(f"使用 {latest_date} 的数据，共 {len(df_today)} 只")
        
        df_today.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
        for _, r in df_today.iterrows():
            print(f"  {r['股票代码']} {r['股票名称']}")
        
        return df_today.to_dict('records')
    return []


if __name__ == '__main__':
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    select_stocks_realtime()
