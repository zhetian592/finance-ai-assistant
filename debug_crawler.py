#!/usr/bin/env python3
"""
调试脚本：只测试爬虫和数据存储，不调用 AI
"""

import os
import sys
import json
import logging
from datetime import datetime

# 导入你的模块
from database import Database
from crawler import NewsCrawler, FundDataCrawler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("===== 爬虫调试模式 - 不调用 AI =====")
    
    # 初始化数据库
    db = Database('data/finance.db')
    db.initialize()
    
    # 1. 爬取新闻
    logger.info("开始爬取新闻...")
    news_crawler = NewsCrawler()
    news_list = news_crawler.fetch_all_sources(max_news=30)
    logger.info(f"共爬取到 {len(news_list)} 条新闻")
    
    if news_list:
        logger.info("新闻示例（前3条）：")
        for i, news in enumerate(news_list[:3]):
            logger.info(f"  {i+1}. {news['title']} ({news['source']})")
    
    # 存储新闻（去重）
    new_count = 0
    for news in news_list:
        nid = db.insert_news(news)
        if nid:
            new_count += 1
    logger.info(f"新增新闻 {new_count} 条")
    
    # 2. 爬取资金数据
    logger.info("开始爬取资金数据...")
    fund_crawler = FundDataCrawler()
    fund_data = fund_crawler.fetch_all()
    db.insert_fund_flow(fund_data)
    
    north = fund_data.get('north_flow', {})
    if north:
        logger.info(f"北向资金: {north.get('date')} 净流入 {north.get('net_inflow')} 亿元")
    else:
        logger.warning("北向资金获取失败")
    
    sectors = fund_data.get('sector_flows', [])
    if sectors:
        logger.info(f"板块资金流入 Top3:")
        for s in sectors[:3]:
            logger.info(f"  {s['sector']}: {s['net_inflow']}")
    else:
        logger.warning("板块资金获取失败")
    
    # 3. 爬取大盘数据
    logger.info("开始爬取大盘数据...")
    market = fund_crawler.fetch_market_data()
    if market:
        db.insert_market_data(market)
        logger.info(f"上证指数: {market['date']} 收盘 {market['close']}")
    else:
        logger.warning("大盘数据获取失败")
    
    # 4. 验证数据库中的新闻
    rows = db.get_latest_news(limit=5)
    logger.info(f"数据库当前最新 {len(rows)} 条新闻")
    for r in rows:
        logger.info(f"  - {r['title'][:50]} | {r['source']} | {r['time']}")
    
    logger.info("===== 爬虫调试完成 =====")

if __name__ == '__main__':
    main()
