#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取主力合约的历史分钟行情数据

配置文件说明 (config.json):
  - save_as_csv: 是否保存为CSV文件 (true/false)
  - frequencies: 分钟级别列表，如 ["1min", "30min"]
  - max_records: 每次获取的最大记录数（默认8000）
  - history_dir: 历史数据存储目录
  - contracts_file: 主力合约列表文件

使用方法：
  python3 get_futures_history_data.py
"""

import tushare
import pandas as pd
import os
import json
from datetime import datetime, timedelta


# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 配置文件路径
CONFIG_FILE = '/Users/qiyue/Desktop/test/claw/quant/history_config.json'


def load_config():
    """加载配置文件"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        # 默认配置
        return {
            "save_as_csv": True,
            "frequencies": ["1min", "30min"],
            "max_records": 8000,
            "history_dir": "/Users/qiyue/Desktop/test/claw/quant/data/history_futures",
            "contracts_file": "/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv"
        }


def get_futures_history(pro, ts_code, freq, start_date, end_date):
    """获取期货历史分钟数据"""
    try:
        df = pro.ft_mins(ts_code=ts_code, freq=freq, 
                         start_date=start_date, 
                         end_date=end_date)
        return df
    except Exception as e:
        print(f"    获取 {ts_code} ({freq}) 失败: {e}")
        return pd.DataFrame()


def main():
    print("=" * 60)
    print("期货历史分钟行情数据采集")
    print("=" * 60)
    
    # 加载配置
    config = load_config()
    
    # 检查是否启用
    if not config.get("save_as_csv", True):
        print("配置中已禁用数据存储，程序退出。")
        print(f"save_as_csv = {config.get('save_as_csv')}")
        return
    
    # 获取配置参数
    frequencies = config.get("frequencies", ["1min", "30min"])
    max_records = config.get("max_records", 8000)
    history_dir = config.get("history_dir", "/Users/qiyue/Desktop/test/claw/quant/data/history_futures")
    contracts_file = config.get("contracts_file", "/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv")
    
    print(f"配置信息:")
    print(f"  - 保存CSV: {config.get('save_as_csv')}")
    print(f"  - 分钟级别: {frequencies}")
    print(f"  - 最大记录数: {max_records}")
    print(f"  - 历史数据目录: {history_dir}")
    print(f"  - 合约文件: {contracts_file}")
    print()
    
    # 读取主力合约列表
    if not os.path.exists(contracts_file):
        print(f"错误: 找不到合约文件 {contracts_file}")
        return
    
    contracts_df = pd.read_csv(contracts_file)
    print(f"读取到 {len(contracts_df)} 个主力合约")
    
    # 创建历史数据目录
    os.makedirs(history_dir, exist_ok=True)
    
    # 初始化 tushare
    pro = tushare.pro_api(TOKEN)
    
    # 计算日期范围
    # 获取8000条数据对应的日期范围（按频率估算）
    # 1min: 约33个交易日 * 4小时 * 60分钟 = 8000
    # 30min: 约83个交易日 * 8个30分钟 = 8000
    end_date = datetime.now()
    
    # 根据频率计算开始日期（大约覆盖8000条数据）
    freq_days = {
        "1min": 40,    # 约40天
        "5min": 200,   # 约200天
        "15min": 400,  # 约400天
        "30min": 500,  # 约500天
        "60min": 1000  # 约1000天
    }
    
    total_success = 0
    total_fail = 0
    
    # 遍历每个频率
    for freq in frequencies:
        # 转换为小写（接口要求小写）
        freq_lower = freq.lower()
        
        print(f"\n{'='*60}")
        print(f"获取 {freq} 数据...")
        print(f"{'='*60}")
        
        # 计算该频率的日期范围
        days = freq_days.get(freq.lower(), 40)
        start_date = (end_date - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        print(f"日期范围: {start_date} ~ {end_date_str}")
        
        # 创建频率子目录
        freq_dir = os.path.join(history_dir, freq)
        os.makedirs(freq_dir, exist_ok=True)
        
        # 遍历每个合约
        for idx, row in contracts_df.iterrows():
            ts_code = row['ts_code']
            
            print(f"[{idx+1}/{len(contracts_df)}] {ts_code}...", end=" ")
            
            # 获取历史数据（使用小写频率）
            df = get_futures_history(pro, ts_code, freq_lower, start_date, end_date_str)
            
            if df is not None and len(df) > 0:
                # 重命名列
                df = df.rename(columns={'trade_time': 'time'})
                
                # 保存到CSV
                filename = f"{ts_code.replace('.', '_')}_{freq_lower}.csv"
                output_file = os.path.join(freq_dir, filename)
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"成功, {len(df)} 条 -> {filename}")
                total_success += 1
            else:
                print("无数据")
                total_fail += 1
    
    print()
    print("=" * 60)
    print(f"完成!")
    print(f"  - 成功: {total_success}")
    print(f"  - 失败/无数据: {total_fail}")
    print(f"数据保存在: {history_dir}")
    print("=" * 60)


if __name__ == '__main__':
    main()
