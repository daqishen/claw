#!/usr/bin/env python3
"""
获取最近与智谱相关的新闻
"""

import tushare as ts
import pandas as pd
from datetime import datetime, timedelta

# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'

# 设置token
ts.set_token(TOKEN)
pro = ts.pro_api()

# 获取最近7天的新闻
end_time = datetime.now()
start_time = end_time - timedelta(days=7)

keywords = '智谱'
sources = ['sina', 'wallstreetcn', 'eastmoney', 'cls', 'yicai']

# 来源名称映射
src_names = {
    'sina': '新浪财经',
    'wallstreetcn': '华尔街见闻', 
    'eastmoney': '东方财富',
    'cls': '财联社',
    'yicai': '第一财经'
}

print(f"搜索关键词: {keywords}")
print(f"时间范围: {start_time.strftime('%Y-%m-%d')} - {end_time.strftime('%Y-%m-%d')}")
print("-" * 50)

all_news = []

for src in sources:
    try:
        print(f"正在查询 {src_names.get(src, src)}...")
        df = pro.news(
            src=src,
            start_date=start_time.strftime('%Y-%m-%d %H:%M:%S'),
            end_date=end_time.strftime('%Y-%m-%d %H:%M:%S')
        )
        if df is not None and len(df) > 0:
            # 过滤包含智谱的新闻
            zhipu_news = df[
                df['content'].str.contains(keywords, na=False) | 
                df['title'].str.contains(keywords, na=False)
            ]
            if len(zhipu_news) > 0:
                zhipu_news['src_name'] = src_names.get(src, src)
                all_news.append(zhipu_news)
                print(f"  -> 找到 {len(zhipu_news)} 条")
    except Exception as e:
        print(f"  -> 出错: {e}")

if all_news:
    result = pd.concat(all_news, ignore_index=True)
    result = result.drop_duplicates(subset=['title'])
    result = result.head(10)
    
    # 保存到CSV
    output_file = 'zhipu_news.csv'
    result[['datetime', 'title', 'content', 'src_name']].to_csv(
        output_file, index=False, encoding='utf-8-sig'
    )
    
    print("-" * 50)
    print(f"✅ 共找到 {len(result)} 条与智谱相关的新闻\n")
    
    for i, row in result.iterrows():
        title = str(row['title'])[:100] if pd.notna(row['title']) else '无标题'
        content = str(row['content'])[:200] if pd.notna(row['content']) else '无内容'
        
        print(f"{i+1}. 【{row['src_name']}】{row['datetime']}")
        print(f"   标题: {title}")
        print(f"   内容: {content}...")
        print()
    
    print(f"📁 数据已保存至: {output_file}")
else:
    print("未找到相关新闻")
