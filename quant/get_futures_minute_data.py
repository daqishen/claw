#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取主力合约的期货分钟行情数据并保存到CSV

配置文件说明 (config.json):
  - save_as_csv: 是否保存为CSV文件 (true/false)
  - frequency: 分钟级别 (1MIN, 5MIN, 15MIN, 30MIN, 60MIN)
  - data_dir: 数据存储目录
  - contracts_file: 主力合约列表文件

使用方法：
  python3 get_futures_minute_data.py
"""

import tushare
import pandas as pd
import os
import json
from datetime import datetime


# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 配置文件路径
CONFIG_FILE = '/Users/qiyue/Desktop/test/claw/quant/config.json'


def load_config():
    """加载配置文件"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return {
            "save_as_csv": True,
            "frequency": "1MIN",
            "data_dir": "/Users/qiyue/Desktop/test/claw/quant/data/futures",
            "contracts_file": "/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv"
        }


def get_futures_minute(ts_code, freq):
    """获取期货分钟行情"""
    pro = tushare.pro_api(TOKEN)
    
    try:
        df = pro.rt_fut_min(ts_code=ts_code, freq=freq)
        return df
    except Exception as e:
        print(f"  获取 {ts_code} 失败: {e}")
        return pd.DataFrame()


def main():
    print("=" * 60)
    print("期货实时分钟行情数据采集")
    print("=" * 60)
    
    # 加载配置
    config = load_config()
    
    # 检查是否启用
    if not config.get("save_as_csv", True):
        print("配置中已禁用数据存储，程序退出。")
        print(f"save_as_csv = {config.get('save_as_csv')}")
        return
    
    # 获取配置参数
    freq = config.get("frequency", "1MIN")
    data_dir = config.get("data_dir", "/Users/qiyue/Desktop/test/claw/quant/data/futures")
    contracts_file = config.get("contracts_file", "/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv")
    
    print(f"配置信息:")
    print(f"  - 保存CSV: {config.get('save_as_csv')}")
    print(f"  - 分钟级别: {freq}")
    print(f"  - 数据目录: {data_dir}")
    print(f"  - 合约文件: {contracts_file}")
    print()
    
    # 读取主力合约列表
    if not os.path.exists(contracts_file):
        print(f"错误: 找不到合约文件 {contracts_file}")
        return
    
    contracts_df = pd.read_csv(contracts_file)
    print(f"读取到 {len(contracts_df)} 个主力合约")
    
    # 创建数据目录
    os.makedirs(data_dir, exist_ok=True)
    print(f"数据目录: {data_dir}")
    print(f"频率: {freq}")
    print(f"日期: {datetime.now().strftime('%Y-%m-%d')}")
    print()
    
    # 遍历每个合约
    success_count = 0
    fail_count = 0
    
    for idx, row in contracts_df.iterrows():
        ts_code = row['ts_code']
        
        print(f"[{idx+1}/{len(contracts_df)}] 获取 {ts_code} ({freq})...", end=" ")
        
        # 获取分钟行情数据
        df = get_futures_minute(ts_code, freq)
        
        if df is not None and len(df) > 0:
            df = df.rename(columns={'code': 'ts_code'})
            output_file = os.path.join(data_dir, f"{ts_code.replace('.', '_')}_{freq}.csv")
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"成功, {len(df)} 条 -> {os.path.basename(output_file)}")
            success_count += 1
        else:
            print("无数据")
            fail_count += 1
    
    print()
    print("=" * 60)
    print(f"完成! 成功: {success_count}, 失败/无数据: {fail_count}")
    print(f"数据保存在: {data_dir}")
    print("=" * 60)


if __name__ == '__main__':
    main()
