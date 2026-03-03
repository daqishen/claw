#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取期货可交易合约并保存到CSV

说明：
- 使用 tushare fut_basic 接口获取各交易所期货合约
- 过滤掉已退市的合约
- 保留所有未退市的合约（供选择）
- 每次运行会覆盖同名文件

使用方法：
  python3 get_main_contracts.py
"""

import tushare
import pandas as pd
import os
from datetime import datetime


# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 输出文件路径
OUTPUT_FILE = '/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv'

# 交易所列表
EXCHANGES = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']


def get_all_contracts():
    """获取所有交易所的有效合约"""
    
    # 初始化 tushare
    pro = tushare.pro_api(TOKEN)
    
    all_contracts = []
    
    print("获取各交易所合约...")
    
    for exchange in EXCHANGES:
        try:
            # 获取所有合约 (fut_type='1')
            df = pro.fut_basic(exchange=exchange, fut_type='1')
            
            if df is not None and len(df) > 0:
                all_contracts.append(df)
                print(f"  {exchange}: {len(df)} 个合约")
                
        except Exception as e:
            print(f"  {exchange}: 获取失败 - {e}")
    
    if not all_contracts:
        print("错误：未能获取任何合约数据")
        return pd.DataFrame()
    
    # 合并所有数据
    result_df = pd.concat(all_contracts, ignore_index=True)
    
    return result_df


def filter_active_contracts(df: pd.DataFrame) -> pd.DataFrame:
    """过滤出未退市的合约"""
    
    if len(df) == 0:
        return df
    
    # 过滤掉已退市的合约
    current_date = datetime.now().strftime('%Y%m%d')
    if 'delist_date' in df.columns:
        df = df[df['delist_date'].fillna('20990101') >= current_date]
    
    return df


def main():
    print("=" * 60)
    print("开始获取期货可交易合约...")
    print("=" * 60)
    
    # 获取所有合约
    all_df = get_all_contracts()
    
    if len(all_df) == 0:
        print("未能获取合约数据")
        return
    
    print(f"\n共获取 {len(all_df)} 个合约")
    
    # 过滤未退市
    active_df = filter_active_contracts(all_df)
    print(f"未退市合约: {len(active_df)} 个")
    
    # 保存到 CSV
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    active_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f"\n已保存到: {OUTPUT_FILE}")
    print("\n各交易所合约数量:")
    print(active_df['exchange'].value_counts())
    
    # 显示玻璃合约
    fg = active_df[active_df['fut_code'] == 'FG']
    if len(fg) > 0:
        print(f"\n玻璃(FG)可交易合约 ({len(fg)} 个):")
        # 按交割月排序
        fg = fg.sort_values('ts_code')
        print(fg[['ts_code', 'name', 'delist_date']].to_string(index=False))
    
    # 显示当前主力合约（选择最近到期的）
    print("\n" + "=" * 60)
    print("推荐主力合约（最近到期）:")
    print("=" * 60)
    
    main_contracts = []
    for fut_code in active_df['fut_code'].unique():
        contracts = active_df[active_df['fut_code'] == fut_code]
        if len(contracts) > 0:
            # 选择最近到期的
            main_contract = contracts.sort_values('ts_code').iloc[0]
            main_contracts.append(main_contract.to_dict())
    
    main_df = pd.DataFrame(main_contracts)
    main_df = main_df.sort_values(['exchange', 'fut_code'])
    
    main_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f"\n主力合约数量: {len(main_df)}")
    print(main_df[['exchange', 'fut_code', 'ts_code', 'name']].head(20).to_string(index=False))
    
    # 玻璃
    fg_main = main_df[main_df['fut_code'] == 'FG']
    if len(fg_main) > 0:
        print(f"\n玻璃主力: {fg_main.iloc[0]['ts_code']}")


if __name__ == '__main__':
    main()
