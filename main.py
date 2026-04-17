#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
财经AI决策辅助工具 - 主入口
适配 GitHub Actions 无头运行
"""

import os
import sys
import json
import logging
from datetime import datetime

from database import Database
from crawler import NewsCrawler, FundDataCrawler
from analyzer import DeepSeekAnalyzer
from signal_engine import SignalEngine
from report_generator import ReportGenerator

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """加载配置文件"""
    config_path = 'config.json'
    if not os.path.exists(config_path):
        logger.error("配置文件 config.json 不存在")
        sys.exit(1)
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    logger.info("===== 财经AI决策辅助工具开始运行 =====")
    
    # 加载配置
    config = load_config()
    
    # 初始化数据库
    db = Database(db_path='data/finance.db')
    db.initialize()
    
    # 爬取新闻
    logger.info("开始爬取新闻...")
    news_crawler = NewsCrawler(config.get('proxy', None))
    news_list = news_crawler.fetch_all_sources(max_news=30)
    logger.info(f"共爬取到 {len(news_list)} 条新闻")
    
    # 保存新闻到数据库（去重）
    new_news_ids = []
    for news in news_list:
        news_id = db.insert_news(news)
        if news_id:
            new_news_ids.append((news_id, news))
    
    logger.info(f"新增未分析新闻 {len(new_news_ids)} 条")
    
    # 爬取资金数据
    logger.info("开始爬取资金数据...")
    fund_crawler = FundDataCrawler(config.get('proxy', None))
    fund_data = fund_crawler.fetch_all()
    db.insert_fund_flow(fund_data)
    
    # 爬取大盘数据
    logger.info("开始爬取大盘数据...")
    market_data = fund_crawler.fetch_market_data()
    db.insert_market_data(market_data)
    
    # AI 分析
    api_key = os.environ.get('DEEPSEEK_API_KEY')
    if not api_key:
        logger.error("未设置环境变量 DEEPSEEK_API_KEY")
        sys.exit(1)
    
    analyzer = DeepSeekAnalyzer(api_key, config.get('deepseek', {}))
    
    for news_id, news in new_news_ids:
        logger.info(f"分析新闻 [{news_id}]: {news['title'][:50]}...")
        analysis = analyzer.analyze_news(news)
        if analysis:
            db.insert_analysis(news_id, analysis)
    
    # 信号合成
    signal_engine = SignalEngine(db, config.get('signal', {}))
    signals = signal_engine.generate_signals()
    logger.info(f"生成信号 {len(signals)} 条")
    
    # 生成 HTML 报告
    logger.info("生成 HTML 报告...")
    report_gen = ReportGenerator(db, config.get('report', {}))
    report_gen.generate()
    
    logger.info("===== 运行完成 =====")

if __name__ == '__main__':
    main()
