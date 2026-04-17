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
    """预检查 API Key 是否有效（最小化请求）"""
    from openai import OpenAI
    try:
        client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1", timeout=10)
        # 发送极简请求测试
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
    
    # 1. 加载配置
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"读取 config.json 失败: {e}")
        sys.exit(1)
    
    # 2. 检查 API Key 是否存在（但暂不验证有效性）
    api_key = os.environ.get('DEEPSEEK_API_KEY')
    if not api_key:
        logger.error("未设置环境变量 DEEPSEEK_API_KEY")
        sys.exit(1)
    
    # 3. 初始化数据库
    db = Database('data/finance.db')
    db.initialize()
    
    # 4. 爬取新闻（必须先成功）
    logger.info("开始爬取新闻...")
    news_crawler = NewsCrawler()
    news_list = news_crawler.fetch_all_sources(max_news=30)
    logger.info(f"共爬取到 {len(news_list)} 条新闻")
    
    if len(news_list) == 0:
        logger.error("未抓取到任何新闻，终止运行，不调用 AI")
        report_gen = ReportGenerator(db, config.get('report', {}))
        report_gen.generate_error_report("新闻抓取失败（0条），请检查网络或数据源接口")
        sys.exit(1)
    
    # 5. 存入数据库，获取新增新闻ID（去重）
    new_ids = []
    for news in news_list:
        nid = db.insert_news(news)
        if nid:
            new_ids.append((nid, news))
    logger.info(f"新增未分析新闻 {len(new_ids)} 条")
    
    # 6. 爬取资金数据和大盘（不影响主流程，失败也继续）
    logger.info("开始爬取资金数据...")
    fund_crawler = FundDataCrawler()
    fund_data = fund_crawler.fetch_all()
    db.insert_fund_flow(fund_data)
    market = fund_crawler.fetch_market_data()
    db.insert_market_data(market)
    
    # 7. 验证 API Key 有效性（只有新闻存在时才验证，避免无效调用）
    if not check_api_key(api_key):
        logger.error("API Key 无效或余额不足，跳过 AI 分析")
        report_gen = ReportGenerator(db, config.get('report', {}))
        report_gen.generate_error_report("DeepSeek API Key 无效或余额不足，请检查 Secrets")
        sys.exit(1)
    
    # 8. AI 分析
    analyzer = DeepSeekAnalyzer(api_key, config.get('deepseek', {}))
    for nid, news in new_ids:
        logger.info(f"分析新闻 [{nid}]: {news['title'][:40]}...")
        analysis = analyzer.analyze_news(news)
        db.insert_analysis(nid, analysis)
    
    # 9. 生成信号
    signal_engine = SignalEngine(db, config.get('signal', {}))
    signals = signal_engine.generate_signals()
    logger.info(f"生成信号 {len(signals)} 条")
    
    # 10. 生成正常报告
    report_gen = ReportGenerator(db, config.get('report', {}))
    report_gen.generate(signals=signals, news_count=len(news_list), api_ok=True)
    
    logger.info("===== 运行完成 =====")

if __name__ == '__main__':
    main()
