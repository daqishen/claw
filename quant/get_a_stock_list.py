#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取A股所有股票名称和代码
"""

import tushare as ts
import pandas as pd
import os

# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 设置token
ts.set_token(TOKEN)
pro = ts.pro_api()

# 输出目录
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/result'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("="*60)
print("获取A股所有股票名称和代码")
print("="*60)

# 获取A股列表
# list_status: L=上市, D=退市, P=暂停上市
# exchange: 上交所 SSE, 深交所 SZSE, 北交所 BSE

all_stocks = []

# 获取沪市A股
print("\n获取沪市A股...")
df_sh = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
if df_sh is not None:
    all_stocks.append(df_sh)
    print(f"  沪市: {len(df_sh)} 只")

# 获取深市A股
print("获取深市A股...")
df_sz = pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
if df_sz is not None:
    all_stocks.append(df_sz)
    print(f"  深市: {len(df_sz)} 只")

# 获取北交所A股
print("获取北交所A股...")
df_bj = pro.stock_basic(exchange='BSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
if df_bj is not None:
    all_stocks.append(df_bj)
    print(f"  北交所: {len(df_bj)} 只")

# 合并所有数据
if all_stocks:
    df_all = pd.concat(all_stocks, ignore_index=True)
    
    # 整理数据
    df_result = df_all[['ts_code', 'symbol', 'name', 'area', 'industry', 'list_date']].copy()
    
    # 添加交易所标识
    df_result['exchange'] = df_result['ts_code'].apply(
        lambda x: '沪市' if x.endswith('.SH') else ('深市' if x.endswith('.SZ') else '北交所')
    )
    
    # 排序：按交易所，然后按代码
    df_result = df_result.sort_values(['exchange', 'ts_code'])
    
    # 保存CSV
    output_file = os.path.join(OUTPUT_DIR, 'a_stock_list.csv')
    df_result.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n共获取 {len(df_result)} 只A股")
    print(f"已保存到: {output_file}")
    
    # 显示前10只
    print("\n前10只股票:")
    print(df_result.head(10).to_string(index=False))
else:
    print("未获取到任何数据")
