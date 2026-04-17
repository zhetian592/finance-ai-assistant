#!/usr/bin/env python3
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_api_key(api_key):
    """预检查 API Key 是否有效"""
    from openai import OpenAI
    try:
        client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1")
        # 发送一个极简请求测试
        client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": "test"}],
            max_tokens=1
        )
        return True
    except Exception as e:
        logger.error(f"API Key 无效: {e}")
        return False

def main():
    logger.info("===== 财经AI决策辅助工具开始运行 =====")
    
    # 读取配置
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    # 检查 API Key
    api_key = os.environ.get('DEEPSEEK_API_KEY')
    if not api_key:
        logger.error("未设置环境变量 DEEPSEEK_API_KEY")
        sys.exit(1)
    if not check_api_key(api_key):
        logger.error("API Key 验证失败，请检查 Secrets 中的 DEEPSEEK_API_KEY 是否正确")
        sys.exit(1)
    logger.info("API Key 验证通过")
    
    # 数据库
    db = Database('data/finance.db')
    db.initialize()
    
    # 爬取新闻
    logger.info("开始爬取新闻...")
    news_crawler = NewsCrawler()
    news_list = news_crawler.fetch_all_sources(max_news=30)
    logger.info(f"共爬取到 {len(news_list)} 条新闻")
    
    new_ids = []
    for news in news_list:
        nid = db.insert_news(news)
        if nid:
            new_ids.append((nid, news))
    logger.info(f"新增未分析新闻 {len(new_ids)} 条")
    
    # 爬取资金数据（降级，失败不影响）
    logger.info("开始爬取资金数据...")
    fund_crawler = FundDataCrawler()
    fund_data = fund_crawler.fetch_all()
    db.insert_fund_flow(fund_data)
    market = fund_crawler.fetch_market_data()
    db.insert_market_data(market)
    
    # AI 分析
    analyzer = DeepSeekAnalyzer(api_key, config.get('deepseek', {}))
    for nid, news in new_ids:
        logger.info(f"分析新闻 [{nid}]: {news['title'][:40]}...")
        analysis = analyzer.analyze_news(news)
        db.insert_analysis(nid, analysis)
    
    # 生成信号
    signal_engine = SignalEngine(db, config.get('signal', {}))
    signals = signal_engine.generate_signals()
    logger.info(f"生成信号 {len(signals)} 条")
    
    # 生成报告
    report_gen = ReportGenerator(db, config.get('report', {}))
    report_gen.generate(signals=signals, news_count=len(news_list), api_ok=True)
    
    logger.info("===== 运行完成 =====")

if __name__ == '__main__':
    main()
