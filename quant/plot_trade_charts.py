#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import tushare as ts
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 设置token
ts.set_token('9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59')
pro = ts.pro_api()

# 读取交易记录
trades_df = pd.read_csv('result/stock_all_buy_points.csv')

stock_605299 = trades_df[trades_df['股票代码'] == '605299.SH'].copy()
stock_002736 = trades_df[trades_df['股票代码'] == '002736.SZ'].copy()

print(f"605299.SH: {len(stock_605299)} trades")
print(f"002736.SZ: {len(stock_002736)} trades")

def get_stock_data(ts_code, start_date, end_date):
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.sort_values('trade_date')
    df.set_index('trade_date', inplace=True)
    return df

def plot_trade_chart(ts_code, trade_records, stock_name, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    
    # 获取所有交易日期的范围
    all_start_dates = []
    all_end_dates = []
    for _, row in trade_records.iterrows():
        td = pd.to_datetime(row['买点日期'])
        all_start_dates.append((td - pd.Timedelta(days=25)).strftime('%Y%m%d'))
        all_end_dates.append((td + pd.Timedelta(days=25)).strftime('%Y%m%d'))
    
    min_date = min(all_start_dates)
    max_date = max(all_end_dates)
    
    print(f"Fetching data from {min_date} to {max_date}...")
    df = get_stock_data(ts_code, min_date, max_date)
    
    if df.empty:
        print(f"No data for {ts_code}")
        return
    
    df['ma5'] = df['close'].rolling(window=5).mean()
    df['ma10'] = df['close'].rolling(window=10).mean()
    df['vol_ma5'] = df['vol'].rolling(window=5).mean()
    
    count = 0
    for idx, row in trade_records.iterrows():
        trade_date = row['买点日期']
        trade_price = row['买入价']
        result = row['5%止盈止损结果']
        
        td_dt = pd.to_datetime(trade_date)
        start_idx = td_dt - pd.Timedelta(days=25)
        end_idx = td_dt + pd.Timedelta(days=25)
        
        plot_df = df[(df.index >= start_idx) & (df.index <= end_idx)].copy()
        
        if len(plot_df) < 5:
            print(f"Skipping {trade_date} - insufficient data")
            continue
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), gridspec_kw={'height_ratios': [3, 1]})
        fig.suptitle(f'{stock_name} ({ts_code}) - Date: {trade_date} | Price: {trade_price:.2f} | Result: {result}', fontsize=14)
        
        # K线
        plot_df_reset = plot_df.reset_index()
        plot_df_reset['date_num'] = mdates.date2num(plot_df_reset['trade_date'])
        ohlc = plot_df_reset[['date_num', 'open', 'high', 'low', 'close']].values
        candlestick_ohlc(ax1, ohlc, width=0.6, colorup='red', colordown='green')
        
        # MA
        ax1.plot(plot_df.index, plot_df['ma5'], label='MA5', color='purple', linewidth=1)
        ax1.plot(plot_df.index, plot_df['ma10'], label='MA10', color='orange', linewidth=1)
        
        # 开仓点
        if td_dt in plot_df.index:
            ax1.axvline(x=td_dt, color='blue', linestyle='--', alpha=0.7)
            ax1.scatter([td_dt], [trade_price], color='blue', s=100, zorder=5, marker='^')
        
        # 止盈止损线
        tp_price = trade_price * 1.05
        sl_price = trade_price * 0.95
        ax1.axhline(y=tp_price, color='green', linestyle='-.', alpha=0.7, label=f'TP {tp_price:.2f}')
        ax1.axhline(y=sl_price, color='red', linestyle='-.', alpha=0.7, label=f'SL {sl_price:.2f}')
        
        ax1.set_ylabel('Price', fontsize=12)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # 成交量
        colors = ['red' if plot_df.loc[d, 'close'] >= plot_df.loc[d, 'open'] else 'green' 
                  for d in plot_df.index]
        ax2.bar(plot_df.index, plot_df['vol'], color=colors, alpha=0.7)
        ax2.plot(plot_df.index, plot_df['vol_ma5'], color='blue', linewidth=1, label='VOL MA5')
        
        if td_dt in plot_df.index:
            ax2.axvline(x=td_dt, color='blue', linestyle='--', alpha=0.7)
        
        ax2.set_ylabel('Volume', fontsize=12)
        ax2.set_xlabel('Date', fontsize=12)
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        
        plt.xticks(rotation=45)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        
        plt.tight_layout()
        
        filename = f'{ts_code}_{trade_date}_{result}.png'
        filepath = os.path.join(save_dir, filename)
        plt.savefig(filepath, dpi=100, bbox_inches='tight')
        plt.close()
        
        count += 1
        print(f"Saved: {filename}")
    
    print(f"Completed {count} charts for {ts_code}")

# 绑制图表
if len(stock_605299) > 0:
    plot_trade_chart('605299.SH', stock_605299, 'Shuhua Sports', 'result/charts/605299_SH')

if len(stock_002736) > 0:
    plot_trade_chart('002736.SZ', stock_002736, 'Guoxin Securities', 'result/charts/002736_SZ')

print("Done!")
