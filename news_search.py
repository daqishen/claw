#!/usr/bin/env python3
"""
Tushare 新闻通讯 Demo
获取伊朗/以色列相关的新闻

使用方法:
    python news_search.py
"""

import requests
import json
from datetime import datetime, timedelta

# Tushare Token
TOKEN = '9d8b0b27c0d38d3274582aae188836b0a8f90c657140a5f263001e59'


def get_major_news(start_date, end_date, fields='title,pub_time,src'):
    """
    获取新闻通讯
    接口: major_news
    
    参数:
        start_date: 开始时间，e.g. "2026-02-25 00:00:00"
        end_date: 结束时间，e.g. "2026-02-28 00:00:00"
        fields: 返回字段
    """
    url = "https://api.tushare.pro"
    
    data = {
        "api_name": "major_news",
        "token": TOKEN,
        "fields": fields,
        "params": {
            "start_date": start_date,
            "end_date": end_date
        }
    }
    
    try:
        resp = requests.post(url, json=data, timeout=30)
        result = resp.json()
        
        if result.get('code') == 0:
            return result.get('data', {}).get('items', [])
        else:
            print(f"API错误: {result.get('msg')}")
            return []
    except Exception as e:
        print(f"请求失败: {e}")
        return []


def filter_keywords(items, keywords):
    """过滤包含关键词的新闻"""
    filtered = []
    for item in items:
        title = item[0]  # title 是第一个字段
        if any(k in title for k in keywords):
            filtered.append(item)
    return filtered


def search_middle_east_news(days=3):
    """
    搜索最近N天的中东相关新闻
    """
    # 计算日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    start_str = start_date.strftime('%Y-%m-%d 00:00:00')
    end_str = end_date.strftime('%Y-%m-%d 00:00:00')
    
    print(f"搜索时间范围: {start_str} ~ {end_str}")
    print("=" * 60)
    
    # 获取新闻
    items = get_major_news(start_str, end_str)
    
    if not items:
        print("未获取到新闻")
        return
    
    print(f"共获取 {len(items)} 条新闻")
    
    # 过滤关键词
    keywords = ['伊朗', '以色列', '中东', '石油', '黄金', '避险', '地缘']
    filtered = filter_keywords(items, keywords)
    
    print(f"找到 {len(filtered)} 条相关新闻")
    print("=" * 60)
    
    for item in filtered:
        title, pub_time, src = item
        print(f"\n【{pub_time}】{src}")
        print(f"标题: {title}")
    
    return filtered


def search_by_keyword(keyword, days=3):
    """
    按关键词搜索新闻
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    start_str = start_date.strftime('%Y-%m-%d 00:00:00')
    end_str = end_date.strftime('%Y-%m-%d 00:00:00')
    
    print(f"搜索关键词: {keyword}")
    print(f"时间范围: {start_str} ~ {end_str}")
    print("=" * 60)
    
    items = get_major_news(start_str, end_str)
    
    if not items:
        print("未获取到新闻")
        return
    
    # 过滤关键词
    filtered = [item for item in items if keyword in item[0]]
    
    print(f"共获取 {len(items)} 条，找到 {len(filtered)} 条相关")
    print("=" * 60)
    
    for item in filtered:
        title, pub_time, src = item
        print(f"\n【{pub_time}】{src}")
        print(f"标题: {title}")


# ============ 主程序 ============

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        # 命令行参数: python news_search.py "伊朗" 3
        keyword = sys.argv[1]
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        search_by_keyword(keyword, days)
    else:
        # 默认搜索中东新闻
        search_middle_east_news(days=3)
