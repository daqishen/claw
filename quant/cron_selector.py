#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日14:30 实时选股定时任务
- 运行选股脚本
- 发送通知到飞书
"""

import subprocess
import json
import os
from datetime import datetime
import sys

# 添加项目路径
sys.path.insert(0, '/Users/qiyue/Desktop/test/claw/quant')

# 运行选股脚本
print("="*60)
print(f"定时任务开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

result = subprocess.run(
    ['python3', '/Users/qiyue/Desktop/test/claw/quant/realtime_selector.py'],
    capture_output=True,
    text=True,
    timeout=300
)

print(result.stdout)
if result.stderr:
    print("ERROR:", result.stderr)

# 读取结果
output_file = f'/Users/qiyue/Desktop/test/claw/quant/result/realtime_buy_points_{datetime.now().strftime("%Y%m%d")}.csv'

if os.path.exists(output_file):
    import pandas as pd
    df = pd.read_csv(output_file)
    count = len(df)
    
    # 构建通知消息
    message = f"📊 **每日实时选股** {datetime.now().strftime('%Y-%m-%d')}\n\n"
    
    if count > 0:
        message += f"找到 **{count}只** 符合买点的股票:\n\n"
        
        # 取前10只显示
        for _, r in df.head(10).iterrows():
            message += f"• {r['ts_code']} {r['name']}: 现价={r['close']}, 涨幅={r['change_15d']}%, 量比={r['vol_ratio']}x\n"
        
        if count > 10:
            message += f"\n...还有 {count-10} 只"
    else:
        message += "今日没有找到符合买点的股票"
    
    message += f"\n\n📁 完整结果: `result/realtime_buy_points_{datetime.now().strftime('%Y%m%d')}.csv`"
else:
    message = f"📊 **每日实时选股** {datetime.now().strftime('%Y-%m-%d')}\n\n⚠️ 选股任务执行完成，但未找到结果文件"

print("\n" + "="*60)
print("通知内容:")
print(message)
print("="*60)

# 保存通知内容供后续发送
with open('/Users/qiyue/Desktop/test/claw/quant/result/cron_notify.txt', 'w') as f:
    f.write(message)

print("\n任务完成")
