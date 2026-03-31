#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最新一日涨停板筛选 + 历史一字涨停次日盈亏统计
- 剔除创业板(300/301)、科创板(688)、北交所(BJ)、ST
- 数据源: 本地历史日线CSV
"""

import pandas as pd
import os
from datetime import datetime

DATA_DIR = '/Users/qiyue/Desktop/test/claw/quant/data/history_stocks'
OUTPUT_DIR = '/Users/qiyue/Desktop/test/claw/quant/result'
STOCK_LIST_FILE = '/Users/qiyue/Desktop/test/claw/quant/result/a_stock_list.csv'


def load_stock_names():
    """加载股票名称"""
    try:
        df = pd.read_csv(STOCK_LIST_FILE)
        return dict(zip(df['ts_code'], df['name']))
    except:
        return {}


def is_valid_stock(ts_code: str, name: str) -> bool:
    """剔除创业板、科创板、北交所、ST"""
    if 'ST' in name or '*ST' in name:
        return False
    if ts_code.startswith('300') or ts_code.startswith('301'):
        return False
    if ts_code.startswith('688'):
        return False
    if ts_code.endswith('.BJ'):
        return False
    return True


def ts_code_from_filename(filename: str) -> str:
    """文件名 -> ts_code"""
    name = filename.replace('.csv', '')
    if name.startswith('sh'):
        return name[2:] + '.SH'
    elif name.startswith('sz'):
        return name[2:] + '.SZ'
    elif name.startswith('bj'):
        return name[2:] + '.BJ'
    return ''


def is_limit_up(close, pre_close, threshold=9.8):
    """判断是否涨停（收盘涨幅>=9.8%）"""
    if pre_close <= 0:
        return False
    return (close - pre_close) / pre_close * 100 >= threshold


def is_yizi_limit_up(row, pre_close):
    """判断是否一字涨停（四价合一 + 涨停）"""
    o, h, l, c = row['open'], row['high'], row['low'], row['close']
    if not (o == h == l == c):
        return False
    return is_limit_up(c, pre_close)


def analyze_stock(filepath: str, ts_code: str):
    """
    分析单只股票:
    1. 检查最新一日是否涨停
    2. 统计历史一字涨停次日盈亏
    """
    try:
        df = pd.read_csv(filepath)
        if df is None or len(df) < 10:
            return None, None

        df = df.sort_values('trade_date').reset_index(drop=True)

        # ---- 1. 最新一日是否涨停 ----
        last_idx = len(df) - 1
        last_row = df.iloc[last_idx]
        pre_close = df.iloc[last_idx - 1]['close']
        latest_date = last_row['trade_date']

        latest_is_limit = is_limit_up(last_row['close'], pre_close)
        latest_is_yizi = is_yizi_limit_up(last_row, pre_close)

        if not latest_is_limit:
            # 最新日不是涨停，跳过
            return None, latest_date

        latest_change = (last_row['close'] - pre_close) / pre_close * 100

        # 量比（vs 前一天）
        pre_vol = df.iloc[last_idx - 1]['vol']
        vol_ratio = last_row['vol'] / pre_vol if pre_vol > 0 else 0

        # 20日均量
        vol_20_avg = df.iloc[max(0, last_idx - 20):last_idx]['vol'].mean()
        vol_ratio_20 = last_row['vol'] / vol_20_avg if vol_20_avg > 0 else 0

        # ---- 2. 历史一字涨停次日盈亏统计 ----
        hist_win = 0      # 次日收涨次数
        hist_lose = 0     # 次日收跌次数
        hist_flat = 0     # 次日收平次数
        hist_limit = 0    # 次日继续涨停次数
        hist_total = 0    # 历史一字涨停总次数
        hist_returns = []  # 所有次日收益率

        for i in range(1, len(df) - 1):
            pc = df.iloc[i - 1]['close']
            row = df.iloc[i]
            nxt = df.iloc[i + 1]

            if is_yizi_limit_up(row, pc):
                hist_total += 1
                ret = (nxt['close'] - row['close']) / row['close'] * 100
                hist_returns.append(ret)

                if ret > 0:
                    hist_win += 1
                elif ret < 0:
                    hist_lose += 1
                else:
                    hist_flat += 1

                if is_limit_up(nxt['close'], row['close']):
                    hist_limit += 1

        result = {
            'ts_code': ts_code,
            'date': latest_date,
            'close': last_row['close'],
            'change_pct': round(latest_change, 2),
            'is_yizi': latest_is_yizi,
            'vol': last_row['vol'],
            'vol_ratio': round(vol_ratio, 2),
            'vol_ratio_20': round(vol_ratio_20, 2),
            'hist_yizi_total': hist_total,
            'hist_win': hist_win,
            'hist_lose': hist_lose,
            'hist_flat': hist_flat,
            'hist_limit_next': hist_limit,
            'hist_win_rate': round(hist_win / (hist_win + hist_lose) * 100, 1) if (hist_win + hist_lose) > 0 else 0,
            'hist_avg_ret': round(sum(hist_returns) / len(hist_returns), 2) if hist_returns else 0,
        }

        return result, latest_date

    except Exception as e:
        return None, None


def main():
    print("=" * 80)
    print("最新一日涨停板筛选 + 历史一字涨停次日盈亏统计")
    print("=" * 80)

    stock_names = load_stock_names()
    print(f"股票名称库: {len(stock_names)} 只")

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"数据文件: {len(files)} 个")

    all_results = []
    latest_date = None
    processed = 0
    skipped = 0

    for f in files:
        ts_code = ts_code_from_filename(f)
        if not ts_code:
            continue

        name = stock_names.get(ts_code, '')
        if not name:
            skipped += 1
            continue
        if not is_valid_stock(ts_code, name):
            skipped += 1
            continue

        filepath = os.path.join(DATA_DIR, f)
        result, date = analyze_stock(filepath, ts_code)

        if date and (not latest_date or date > latest_date):
            latest_date = date

        if result:
            result['name'] = name
            all_results.append(result)

        processed += 1
        if processed % 500 == 0:
            print(f"  已处理 {processed} 只...")

    print(f"\n处理完成: {processed} 只, 跳过 {skipped} 只")
    print(f"最新交易日: {latest_date}")

    if not all_results:
        print("最新一日没有涨停股票!")
        return

    df = pd.DataFrame(all_results)

    # 只保留最新日的涨停
    df = df[df['date'] == latest_date].copy()

    if len(df) == 0:
        print(f"最新交易日 {latest_date} 没有涨停股票!")
        return

    # 排序: 一字板在前，然后按历史一字板次数降序
    df = df.sort_values(['is_yizi', 'hist_yizi_total'], ascending=[False, False])

    # ==================== 输出 ====================
    yizi_df = df[df['is_yizi'] == True]
    non_yizi_df = df[df['is_yizi'] == False]

    print(f"\n最新交易日 {latest_date} 涨停股票: {len(df)} 只")
    print(f"  其中一字涨停: {len(yizi_df)} 只")
    print(f"  非一字涨停: {len(non_yizi_df)} 只")

    # ---- 一字涨停板 ----
    if len(yizi_df) > 0:
        print("\n" + "=" * 80)
        print(f"🔴 一字涨停板 ({len(yizi_df)} 只)")
        print("=" * 80)
        print(f"{'股票':　<10} {'代码':<12} {'收盘':>8} {'涨幅':>7} {'量比':>6} "
              f"{'历史一字':>8} {'次日赢':>6} {'次日亏':>6} {'胜率':>7} {'平均收益':>8} {'次日连板':>8}")
        print("-" * 100)

        for _, row in yizi_df.iterrows():
            print(f"{row['name']:　<8} {row['ts_code']:<12} {row['close']:>8.2f} "
                  f"{row['change_pct']:>6.1f}% {row['vol_ratio_20']:>5.1f}x "
                  f"{row['hist_yizi_total']:>7}次 {row['hist_win']:>5}次 {row['hist_lose']:>5}次 "
                  f"{row['hist_win_rate']:>6.1f}% {row['hist_avg_ret']:>7.2f}% "
                  f"{row['hist_limit_next']:>7}次")

    # ---- 非一字涨停板 ----
    if len(non_yizi_df) > 0:
        print("\n" + "=" * 80)
        print(f"🟡 非一字涨停板 ({len(non_yizi_df)} 只)")
        print("=" * 80)
        print(f"{'股票':　<10} {'代码':<12} {'收盘':>8} {'涨幅':>7} {'量比':>6} "
              f"{'历史一字':>8} {'次日赢':>6} {'次日亏':>6} {'胜率':>7} {'平均收益':>8} {'次日连板':>8}")
        print("-" * 100)

        for _, row in non_yizi_df.iterrows():
            wr = f"{row['hist_win_rate']:.1f}%" if row['hist_yizi_total'] > 0 else "  -"
            ar = f"{row['hist_avg_ret']:.2f}%" if row['hist_yizi_total'] > 0 else "   -"
            print(f"{row['name']:　<8} {row['ts_code']:<12} {row['close']:>8.2f} "
                  f"{row['change_pct']:>6.1f}% {row['vol_ratio_20']:>5.1f}x "
                  f"{row['hist_yizi_total']:>7}次 {row['hist_win']:>5}次 {row['hist_lose']:>5}次 "
                  f"{wr:>7} {ar:>8} "
                  f"{row['hist_limit_next']:>7}次")

    # ---- 保存 ----
    save_cols = ['ts_code', 'name', 'date', 'close', 'change_pct', 'is_yizi',
                 'vol', 'vol_ratio', 'vol_ratio_20',
                 'hist_yizi_total', 'hist_win', 'hist_lose', 'hist_flat',
                 'hist_limit_next', 'hist_win_rate', 'hist_avg_ret']

    output_file = os.path.join(OUTPUT_DIR, 'today_limit_up_stocks.csv')
    df[save_cols].to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n📁 数据已保存: {output_file}")


if __name__ == '__main__':
    main()
