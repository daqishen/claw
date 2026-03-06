#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
刷新主力合约历史数据

逻辑：
1. 筛选最近5个月内的合约
2. 获取最近5个交易日数据
3. 成交量最高的作为主力
4. 获取主力合约的历史数据
"""

import tushare
import pandas as pd
import os
from datetime import datetime, timedelta


TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

OUTPUT_FILE = '/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv'
HISTORY_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_futures/1min'
EXCHANGES = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']


def get_all_contracts():
    """获取所有合约"""
    pro = tushare.pro_api(TOKEN)
    all_contracts = []
    
    for ex in EXCHANGES:
        try:
            df = pro.fut_basic(exchange=ex, fut_type='1')
            if df is not None:
                all_contracts.append(df)
        except:
            pass
    
    return pd.concat(all_contracts, ignore_index=True) if all_contracts else pd.DataFrame()


def extract_ym(ts_code):
    """提取年月"""
    try:
        parts = ts_code.split('.')[0]
        nums = ''.join(filter(str.isdigit, parts))
        if len(nums) >= 4:
            y = int(nums[:2])
            m = int(nums[2:])
            return (2000 + y) * 100 + m if y <= 50 else (1900 + y) * 100 + m
    except:
        pass
    return 999999


def get_main_contract(pro, contracts_df, fut_code):
    """获取某品种的主力合约"""
    code_contracts = contracts_df[contracts_df['fut_code'] == fut_code].copy()
    
    if len(code_contracts) == 0:
        return None
    
    if len(code_contracts) == 1:
        return code_contracts.iloc[0].to_dict()
    
    # 多个合约，选择成交量最大的
    volumes = []
    for _, row in code_contracts.iterrows():
        ts_code = row['ts_code']
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=10)
            df = pro.ft_mins(
                ts_code=ts_code, freq='1min',
                start_date=f'{start_date.strftime("%Y-%m-%d")} 09:00:00',
                end_date=f'{end_date.strftime("%Y-%m-%d")} 15:00:00'
            )
            vol = df['vol'].sum() if df is not None and len(df) > 0 else 0
            volumes.append({'ts_code': ts_code, 'volume': vol})
        except:
            volumes.append({'ts_code': ts_code, 'volume': 0})
    
    vol_df = pd.DataFrame(volumes)
    if len(vol_df) > 0:
        best = vol_df.loc[vol_df['volume'].idxmax()]
        return code_contracts[code_contracts['ts_code'] == best['ts_code']].iloc[0].to_dict()
    
    return code_contracts.iloc[0].to_dict()


def get_history_data(pro, ts_code, days=60):
    """获取历史数据"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        df = pro.ft_mins(
            ts_code=ts_code, freq='1min',
            start_date=f'{start_date.strftime("%Y-%m-%d")} 09:00:00',
            end_date=f'{end_date.strftime("%Y-%m-%d")} 15:00:00'
        )
        return df
    except Exception as e:
        print(f"  获取 {ts_code} 失败: {e}")
        return None


def main():
    print("=" * 60)
    print("刷新主力合约历史数据")
    print("=" * 60)
    
    pro = tushare.pro_api(TOKEN)
    
    # 1. 获取所有合约
    print("\n1. 获取所有合约...")
    all_df = get_all_contracts()
    
    # 2. 过滤未退市（动态获取当前日期）
    current_date = datetime.now().strftime('%Y%m%d')
    all_df = all_df[all_df['delist_date'].fillna('20990101') >= current_date]
    
    # 3. 提取年月，筛选最近6个月
    all_df['ym'] = all_df['ts_code'].apply(extract_ym)
    now = datetime.now()
    current_ym = now.year * 100 + now.month
    six_months_later_ym = ((now.year if now.month <= 6 else now.year + 1) * 100 + (now.month + 5 if now.month <= 7 else now.month - 7))
    all_df = all_df[(all_df['ym'] >= current_ym) & (all_df['ym'] <= six_months_later_ym)]
    print(f"   6个月内合约: {len(all_df)} 个")
    
    # 4. 获取每个品种的主力合约
    print("\n2. 确定主力合约...")
    main_contracts = []
    fut_codes = all_df['fut_code'].unique()
    
    for i, fut_code in enumerate(fut_codes):
        print(f"   [{i+1}/{len(fut_codes)}] {fut_code}...", end=" ", flush=True)
        contract = get_main_contract(pro, all_df, fut_code)
        if contract:
            main_contracts.append(contract)
            print(f"{contract['ts_code']}")
        else:
            print("无")
    
    main_df = pd.DataFrame(main_contracts)
    main_df = main_df.sort_values(['exchange', 'fut_code'])
    
    # 保存主力合约列表
    main_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\n主力合约: {len(main_df)} 个")
    
    # 5. 获取历史数据
    print("\n3. 获取历史数据...")
    os.makedirs(HISTORY_DIR, exist_ok=True)
    
    success = 0
    fail = 0
    
    for i, row in main_df.iterrows():
        ts_code = row['ts_code']
        filename = f"{ts_code.replace('.', '_')}_1min.csv"
        filepath = os.path.join(HISTORY_DIR, filename)
        
        print(f"   [{i+1}/{len(main_df)}] {ts_code}...", end=" ", flush=True)
        
        df = get_history_data(pro, ts_code)
        if df is not None and len(df) > 0:
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"{len(df)} 条")
            success += 1
        else:
            print("无数据")
            fail += 1
    
    print(f"\n完成! 成功: {success}, 失败: {fail}")
    
    # 显示玻璃主力
    fg = main_df[main_df['fut_code'] == 'FG']
    if len(fg) > 0:
        print(f"\n玻璃主力: {fg.iloc[0]['ts_code']}")


if __name__ == '__main__':
    main()
