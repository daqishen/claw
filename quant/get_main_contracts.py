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
from datetime import datetime, timedelta


# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 输出文件路径
OUTPUT_FILE = '/Users/qiyue/Desktop/test/claw/quant/main_contracts.csv'

# 交易所列表
EXCHANGES = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']

# 常用期货品种列表
MAIN_CODES = [
    # 股指期货
    'IF', 'IH', 'IC', 'IM',
    # 国债期货
    'TS', 'TF', 'T', 'TL',
    # 金属期货
    'AU', 'AG', 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'SS',
    # 黑色系
    'RB', 'HC', 'I', 'J', 'JM', 'RU',
    # 能源化工
    'SC', 'LU', 'FU', 'TA', 'MA', 'SA', 'FG', 'RU',
    # 农产品
    'M', 'Y', 'A', 'C', 'P', 'L', 'PP', 'V', 'EG', 'SR', 'CF', 'RM', 'AP',
    # 油脂油料
    'OI', 'PF', 'PK',
    # 其他
    'NR', 'PG', 'EB', 'BZ', 'LH', 'JD', 'CS',
    # 新能源
    'LC', 'SI', 'PS'
]


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


def get_main_contract_by_volume(pro, exchange, fut_code, active_contracts, days=5):
    """
    获取主力合约：6个月内到期 + 近N日成交量最大
    
    Args:
        pro: tushare API对象
        exchange: 交易所代码
        fut_code: 期货品种代码
        active_contracts: 活跃合约DataFrame
        days: 获取成交量数据的日数（默认5天）
    
    Returns:
        主力合约信息dict，包含成交量数据
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
    
    # 获取近N日成交量数据
    start_date = (now - timedelta(days=days+3)).strftime('%Y%m%d')  # 多取几天确保有数据
    
    # 获取每个合约的成交量
    volume_data = []
    for ts_code in contracts['ts_code']:
        try:
            df_daily = pro.fut_daily(ts_code=ts_code, start_date=start_date, end_date=current_date)
            if df_daily is not None and len(df_daily) > 0:
                total_vol = df_daily['vol'].sum()
                volume_data.append({'ts_code': ts_code, 'vol': total_vol})
        except Exception as e:
            pass
    
    if not volume_data:
        # 如果没有成交量数据，返回最近到期的
        main_contract = contracts.sort_values('delist_date').iloc[0]
        return {
            'ts_code': main_contract['ts_code'],
            'exchange': main_contract['exchange'],
            'fut_code': main_contract['fut_code'],
            'name': main_contract['name'],
            'delist_date': main_contract['delist_date'],
            'vol': 0
        }
    
    vol_df = pd.DataFrame(volume_data).sort_values('vol', ascending=False)
    main_ts_code = vol_df.iloc[0]['ts_code']
    main_vol = vol_df.iloc[0]['vol']
    
    main_contract = contracts[contracts['ts_code'] == main_ts_code].iloc[0]
    
    return {
        'ts_code': main_contract['ts_code'],
        'exchange': main_contract['exchange'],
        'fut_code': main_contract['fut_code'],
        'name': main_contract['name'],
        'delist_date': main_contract['delist_date'],
        'vol': int(main_vol)
    }


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
    
    # 显示当前主力合约（6个月内到期 + 近5日成交量最大）
    print("\n" + "=" * 60)
    print("🔥 推荐主力合约（6个月内到期 + 近5日成交量最大）:")
    print("=" * 60)
    
    pro = tushare.pro_api(TOKEN)
    
    # 获取所有品种
    all_fut_codes = active_df['fut_code'].unique()
    
    main_contracts = []
    for fut_code in all_fut_codes:
        contract_sample = active_df[active_df['fut_code'] == fut_code]
        if len(contract_sample) == 0:
            continue
        
        exchange = contract_sample.iloc[0]['exchange']
        main_contract = get_main_contract_by_volume(pro, exchange, fut_code, active_df)
        
        if main_contract is not None:
            main_contracts.append(main_contract)
            print(f"  {fut_code:6} -> {main_contract['ts_code']:12} (成交量: {main_contract['vol']:>12,})")
    
    print(f"\n共计算 {len(main_contracts)} 个品种的主力合约")


if __name__ == '__main__':
    main()
