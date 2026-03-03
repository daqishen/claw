#!/usr/bin/env python3
"""
获取沪深300(000300.SH)近一年日线数据
"""

import tushare as ts
from datetime import datetime, timedelta
import os

# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 设置token
ts.set_token(TOKEN)
pro = ts.pro_api()

# 计算近一年的日期范围
end_date = datetime.now().strftime('%Y%m%d')
start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

print(f"获取沪深300日线数据...")
print(f"日期范围: {start_date} - {end_date}")

try:
    # 获取沪深300指数日线数据
    # 000300.SH 是沪深300指数代码
    df = pro.index_daily(
        ts_code='000300.SH',
        start_date=start_date,
        end_date=end_date
    )
    
    if df is not None and len(df) > 0:
        # 按日期升序排列（便于查看走势）
        df = df.sort_values('trade_date')
        
        # 保存为CSV
        output_path = os.path.join(os.path.dirname(__file__), 'csi300_daily.csv')
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ 成功获取 {len(df)} 条数据")
        print(f"📁 已保存至: {output_path}")
        print(f"\n数据预览:")
        print(df.head())
    else:
        print("❌ 未获取到数据，请检查token或权限")
        
except Exception as e:
    print(f"❌ 出错: {e}")
