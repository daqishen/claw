#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取A股所有股票的日线数据
读取a_stock_list.csv，为每只股票获取近一年的日线数据
"""

import tushare as ts
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 设置token
ts.set_token(TOKEN)
pro = ts.pro_api()

# 路径配置
STOCK_LIST_FILE = '/Users/qiyue/Desktop/test/claw/quant/result/a_stock_list.csv'
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_stocks'

# 创建输出目录
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 获取日期范围
end_date = datetime.now().strftime('%Y%m%d')
start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

print("="*60)
print("获取A股日线数据")
print("="*60)
print(f"日期范围: {start_date} ~ {end_date}")
print(f"输出目录: {OUTPUT_DIR}")

# 读取股票列表
df_stocks = pd.read_csv(STOCK_LIST_FILE)
print(f"\n共 {len(df_stocks)} 只股票")

# 获取已存在的文件
existing_files = set(os.listdir(OUTPUT_DIR)) if os.path.exists(OUTPUT_DIR) else set()
print(f"已存在 {len(existing_files)} 个数据文件")

# 需要获取的股票（包含新增和需要更新的）
stocks_to_fetch = []
stocks_to_update = []  # 需要更新数据的股票

for _, row in df_stocks.iterrows():
    ts_code = row['ts_code']
    # 文件名格式: sz000001.sh, sh600000.sh, bj920000.sh
    if ts_code.endswith('.SH'):
        code = 'sh' + ts_code.replace('.SH', '')
    elif ts_code.endswith('.SZ'):
        code = 'sz' + ts_code.replace('.SZ', '')
    elif ts_code.endswith('.BJ'):
        code = 'bj' + ts_code.replace('.BJ', '')
    else:
        continue
    
    filename = f"{code}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    if filename not in existing_files:
        stocks_to_fetch.append((ts_code, code, 'new'))
    else:
        # 检查现有文件是否需要更新
        try:
            df_existing = pd.read_csv(filepath)
            if 'trade_date' in df_existing.columns:
                latest_date = df_existing['trade_date'].max()
                # 获取今天的日期
                today_str = datetime.now().strftime('%Y%m%d')
                # 如果最新日期不是今天，需要更新
                if latest_date != today_str:
                    stocks_to_update.append((ts_code, code, 'update', latest_date))
        except:
            stocks_to_fetch.append((ts_code, code, 'new'))

print(f"需要新增: {len(stocks_to_fetch)} 只股票")
print(f"需要更新: {len(stocks_to_update)} 只股票")

# 合并所有需要处理的股票
all_stocks = stocks_to_fetch + stocks_to_update
print(f"总共需要处理: {len(all_stocks)} 只股票")

# 批量获取
success_count = 0
fail_count = 0
batch_size = 50  # 每50只打印一次进度

for i, stock_info in enumerate(all_stocks):
    ts_code = stock_info[0]
    code = stock_info[1]
    update_type = stock_info[2] if len(stock_info) > 2 else 'new'
    
    try:
        # 获取日线数据
        df_daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df_daily is not None and len(df_daily) > 0:
            # 转换日期格式
            df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'])
            df_daily = df_daily.sort_values('trade_date')
            
            # 保存CSV
            output_file = os.path.join(OUTPUT_DIR, f"{code}.csv")
            df_daily.to_csv(output_file, index=False, encoding='utf-8-sig')
            success_count += 1
        else:
            fail_count += 1
            
    except Exception as e:
        fail_count += 1
    
    # 打印进度
    if (i + 1) % batch_size == 0:
        print(f"进度: {i+1}/{len(all_stocks)} (成功:{success_count}, 失败:{fail_count})")
    
    # 避免请求过快
    time.sleep(0.1)

print(f"\n完成! 成功:{success_count}, 失败:{fail_count}")
print(f"数据保存在: {OUTPUT_DIR}")
