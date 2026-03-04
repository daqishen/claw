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


def get_main_contract_by_volume(pro, exchange, fut_code, active_contracts):
    """
    获取主力合约：6个月内到期 + 近3日成交量最大
    """
    from datetime import datetime, timedelta
    
    now = datetime.now()
    current_date = now.strftime('%Y%m%d')
    six_months_later = (now + timedelta(days=180)).strftime('%Y%m%d')
    
    # 筛选6个月内到期的合约
    contracts = active_contracts[(active_contracts['exchange'] == exchange) &
                                   (active_contracts['fut_code'] == fut_code) &
                                   (active_contracts['delist_date'] >= current_date) &
                                   (active_contracts['delist_date'] <= six_months_later)]
    
    if len(contracts) == 0:
        return None
    
    # 获取每个合约的成交量
    volume_data = []
    for ts_code in contracts['ts_code']:
        try:
            df_daily = pro.fut_daily(ts_code=ts_code, start_date='20260301', end_date='20260304')
            if df_daily is not None and len(df_daily) > 0:
                total_vol = df_daily['vol'].sum()
                volume_data.append({'ts_code': ts_code, 'vol': total_vol})
        except:
            pass
    
    if not volume_data:
        # 如果没有成交量数据，返回最近到期的
        return contracts.sort_values('delist_date').iloc[0]
    
    vol_df = pd.DataFrame(volume_data).sort_values('vol', ascending=False)
    main_ts_code = vol_df.iloc[0]['ts_code']
    return contracts[contracts['ts_code'] == main_ts_code].iloc[0]


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
    
    # 显示当前主力合约（6个月内到期 + 近3日成交量最大）
    print("\n" + "=" * 60)
    print("推荐主力合约（6个月内到期 + 近3日成交量最大）:")
    print("=" * 60)
    
    # 只计算玻璃、纯碱、螺纹钢、沪金等常用合约
    main_codes = ['FG', 'SA', 'RB', 'AU', 'IF', 'IC', 'IH', 'IM', 'RU', 'TA', 'MA', 'PP', 'PVC', 'J', 'JM', 'I']
    pro = tushare.pro_api(TOKEN)
    
    main_contracts = []
    for fut_code in main_codes:
        # 获取交易所代码
        contract_sample = active_df[active_df['fut_code'] == fut_code]
        if len(contract_sample) == 0:
            continue
        
        exchange = contract_sample.iloc[0]['exchange']
        main_contract = get_main_contract_by_volume(pro, exchange, fut_code, active_df)
        
        if main_contract is not None:
            main_contracts.append(main_contract.to_dict())
            print(f"  {fut_code}: {main_contract['ts_code']}")


if __name__ == '__main__':
    main()
