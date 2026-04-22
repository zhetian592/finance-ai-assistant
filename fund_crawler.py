#!/usr/bin/env python3
"""
财经AI决策辅助工具 - 进阶版
集成：增强事件提取 + 多因子模型 + 组合优化
目标：接近职业基金经理分析能力
"""

import os
import sys
import json
import argparse
import logging
import requests
import feedparser
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# 量化模块
from quant import get_market_risk_level, update_fund_nav, get_risk_advice

# 进阶模块
from enhanced_event_extractor import extract_event
from factor_model import FactorModel
from portfolio_optimizer import PortfolioOptimizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== 配置 ====================
HOLDINGS_FILE = "holdings.json"
SOURCES_FILE = "fund_sources.json"
REPORT_FILE = "fund_report.html"
RECOMMENDATIONS_FILE = "backtest/recommendations.json"
DEFAULT_CASH = 50000

# ==================== 辅助函数 ====================
def load_json(file_path: str, default: Any = None) -> Any:
    if not os.path.exists(file_path):
        return default if default is not None else {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载 {file_path} 失败: {e}")
        return default if default is not None else {}

def save_json(file_path: str, data: Any):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def fetch_rss_feed(url: str, timeout: int = 15) -> List[Dict]:
    try:
        feed = feedparser.parse(url)
        entries = []
        for entry in feed.entries[:10]:
            entries.append({
                "title": entry.get("title", ""),
                "summary": entry.get("summary", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "source": url
            })
        return entries
    except Exception as e:
        logger.warning(f"RSS 抓取失败 {url}: {e}")
        return []

def fetch_all_news(sources: List[str]) -> List[Dict]:
    all_news = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(fetch_rss_feed, url): url for url in sources}
        for future in as_completed(future_to_url):
            entries = future.result()
            all_news.extend(entries)
    seen = set()
    unique = []
    for item in all_news:
        title = item.get("title", "")
        if title and title not in seen:
            seen.add(title)
            unique.append(item)
    return unique

def generate_html_report(holdings, news, recommendations, market_risk, risk_advice, mode, cash):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    risk_level = market_risk.get("level", "unknown")
    risk_color = {"high": "red", "medium": "orange", "low": "green"}.get(risk_level, "gray")
    is_empty = len(holdings) == 0

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>财经AI决策报告 - {timestamp}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #2c3e50; }}
        .risk {{ padding: 10px; border-radius: 5px; margin: 10px 0; }}
        .risk-high {{ background-color: #ffebee; border-left: 5px solid red; }}
        .risk-medium {{ background-color: #fff3e0; border-left: 5px solid orange; }}
        .risk-low {{ background-color: #e8f5e9; border-left: 5px solid green; }}
        table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .news-item {{ margin-bottom: 15px; padding: 10px; background: #f9f9f9; }}
    </style>
</head>
<body>
    <h1>📊 财经AI决策报告（进阶版）</h1>
    <p>生成时间: {timestamp}</p>
    <p>运行模式: {mode}</p>
    <p>可用现金: {cash} 元</p>

    <div class="risk risk-{risk_level}">
        <h2>📈 市场风险评估</h2>
        <p>等级: <strong>{risk_level.upper()}</strong> - {market_risk.get('advice', '')}</p>
        <p>评分: {market_risk.get('score', 0)}</p>
        <ul>"""
    for reason in market_risk.get('reasons', []):
        html += f"<li>{reason}</li>"
    html += f"""
        </ul>
    </div>

    <div class="risk risk-{risk_level}">
        <h2>🛡️ 风控建议</h2>
        <pre>{risk_advice}</pre>
    </div>
"""
    if is_empty:
        html += "<h2>🌟 多因子+组合优化推荐</h2>"
        if recommendations:
            html += "<table><tr><th>基金代码</th><th>基金名称</th><th>建议买入金额（元）</th><th>推荐理由</th></tr>"
            for rec in recommendations:
                html += f"<tr><td>{rec['code']}</td><td>{rec['name']}</td><td>{rec['amount']}</td><td>{rec['reason']}</td></tr>"
            html += "</table>"
            html += f"<p>💡 建议使用现金 {cash} 元，按上述金额配置。组合已考虑市场风险等级{risk_level}。</p>"
        else:
            html += "<p>⚠️ 未提取到有效投资信号，请稍后重试。</p>"
    else:
        html += "<h2>💰 当前持仓</h2><table><tr><th>基金代码</th><th>名称</th><th>持有份额</th><th>成本价</th><th>现价</th><th>浮动盈亏</th></tr>"
        for fund in holdings:
            code = fund.get('code', '')
            name = fund.get('name', '')
            amount = fund.get('amount', 0)
            cost = fund.get('cost', 0)
            current = fund.get('current', 0)
            profit = (current - cost) * amount
            profit_color = "green" if profit >= 0 else "red"
            html += f"<tr><td>{code}</td><td>{name}</td><td>{amount}</td><td>{cost:.4f}</td><td>{current:.4f}</td><td style='color:{profit_color}'>{profit:+.2f}</td></tr>"
        html += "</table><p>📌 有持仓时仅提供市场风控建议，不自动生成买卖指令。</p>"
    
    html += "<h2>📰 近期财经新闻（事件驱动依据）</h2>"
    for item in news[:10]:
        title = item.get('title', '无标题')
        summary = item.get('summary', '')[:200]
        link = item.get('link', '#')
        html += f'<div class="news-item"><a href="{link}" target="_blank"><strong>{title}</strong></a><p>{summary}...</p></div>'
    html += "</body></html>"
    return html

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["recommend", "hold"], default="recommend")
    args = parser.parse_args()

    # 加载持仓
    raw_data = load_json(HOLDINGS_FILE, {"holdings": [], "cash": DEFAULT_CASH})
    if isinstance(raw_data, list):
        holdings_list = raw_data
        cash = DEFAULT_CASH
    else:
        holdings_list = raw_data.get("holdings", [])
        cash = raw_data.get("cash", DEFAULT_CASH)
    logger.info(f"加载持仓 {len(holdings_list)} 只基金，现金 {cash} 元")

    # 更新净值（如有持仓）
    if holdings_list:
        try:
            updated = update_fund_nav(holdings_list)
            if updated:
                holdings_list = updated
                if isinstance(raw_data, list):
                    raw_data = {"holdings": holdings_list, "cash": cash}
                else:
                    raw_data["holdings"] = holdings_list
                save_json(HOLDINGS_FILE, raw_data)
                logger.info("基金净值已更新")
        except Exception as e:
            logger.warning(f"更新净值失败: {e}")

    # 市场风险
    market_risk = get_market_risk_level()
    logger.info(f"市场风险等级: {market_risk.get('level')}")
    risk_advice = get_risk_advice(holdings_list, cash, market_risk)

    # 抓取新闻
    sources = load_json(SOURCES_FILE, [])
    if not sources:
        sources = [
            "https://feeds.bloomberg.com/markets/news.rss",
            "https://feeds.bloomberg.com/economics/news.rss",
            "http://feeds.bbci.co.uk/news/business/rss.xml",
            "https://www.wsj.com/xml/rss/3_7085.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        ]
    logger.info(f"抓取 {len(sources)} 个 RSS 源")
    news = fetch_all_news(sources)
    logger.info(f"抓取到 {len(news)} 条新闻")

    # 事件提取 + 多因子 + 组合优化
    recommendations = []
    if args.mode == "recommend" and not holdings_list:
        logger.info("开始进阶分析：事件提取 -> 多因子评分 -> 组合优化")
        events = []
        for item in news[:20]:
            evt = extract_event(item)
            if evt["industries"]:
                events.append(evt)
        logger.info(f"提取到 {len(events)} 个有效事件")
        if events:
            factor = FactorModel()
            top_industries = factor.recommend_industries(events, top_k=3)
            logger.info(f"推荐行业: {top_industries}")
            if top_industries:
                recommendations = PortfolioOptimizer.allocate_cash(
                    top_industries, cash, market_risk.get("level", "medium")
                )
                logger.info(f"生成 {len(recommendations)} 条配置建议")
        else:
            logger.warning("未提取到有效事件")

    # 生成报告
    html = generate_html_report(holdings_list, news, recommendations, market_risk, risk_advice, args.mode, cash)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"报告已保存至 {REPORT_FILE}")

    # 保存回测记录
    if args.mode == "recommend" and recommendations:
        os.makedirs(os.path.dirname(RECOMMENDATIONS_FILE), exist_ok=True)
        existing = load_json(RECOMMENDATIONS_FILE, [])
        new_record = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "recommendations": recommendations,
            "market_risk": market_risk.get("level"),
            "events_used": len(events) if 'events' in locals() else 0
        }
        existing.append(new_record)
        save_json(RECOMMENDATIONS_FILE, existing)
        logger.info(f"回测记录已追加")
    else:
        if not os.path.exists(RECOMMENDATIONS_FILE):
            save_json(RECOMMENDATIONS_FILE, [])

    logger.info("任务完成")

if __name__ == "__main__":
    main()
