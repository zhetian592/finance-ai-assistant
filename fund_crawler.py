#!/usr/bin/env python3
"""
财经AI决策辅助工具 - 行业轮动版
"""

import os
import sys
import json
import argparse
import logging
import feedparser
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from quant import get_market_risk_level, update_fund_nav, get_risk_advice
from data_fetcher import fetch_all_sector_data, SECTORS
from sector_rotator import allocate_weights_by_score, generate_sector_report, generate_concentration_warning

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HOLDINGS_FILE = "holdings.json"
SOURCES_FILE = "fund_sources.json"
REPORT_FILE = "fund_report.html"
RECOMMENDATIONS_FILE = "backtest/recommendations.json"
DEFAULT_CASH = 50000

def load_json(file_path, default=None):
    if not os.path.exists(file_path):
        return default if default is not None else {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载 {file_path} 失败: {e}")
        return default if default is not None else {}

def save_json(file_path, data):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def fetch_rss_feed(url, timeout=15):
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

def fetch_all_news(sources):
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

def generate_html_report(holdings, news, sector_df, recommendations, market_risk, risk_advice, mode, cash, concentration_warning=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    risk_level = market_risk.get("level", "unknown")
    is_empty = len(holdings) == 0
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>财经AI决策报告 - {timestamp}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
.risk {{ padding: 10px; border-radius: 5px; margin: 10px 0; }}
.risk-high {{ background-color: #ffebee; border-left: 5px solid red; }}
.risk-medium {{ background-color: #fff3e0; border-left: 5px solid orange; }}
.risk-low {{ background-color: #e8f5e9; border-left: 5px solid green; }}
table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background-color: #f2f2f2; }}
.warning {{ background-color: #fff3cd; border-left: 5px solid #ffc107; padding: 10px; margin: 10px 0; }}
</style>
</head>
<body>
<h1>📊 财经AI决策报告</h1>
<p>生成时间: {timestamp}</p>
<p>运行模式: {mode}</p>
<p>可用现金: {cash} 元</p>
<div class="risk risk-{risk_level}">
<h2>📈 市场风险评估</h2>
<p>等级: <strong>{risk_level.upper()}</strong> - {market_risk.get('advice', '')}</p>
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
        if sector_df is not None and not sector_df.empty:
            html += generate_sector_report(sector_df, top_n=8)
        if recommendations:
            html += "<h2>🌟 行业轮动基金推荐</h2>"
            if concentration_warning:
                html += f'<div class="warning">{concentration_warning}</div>'
            html += "<table><th>基金代码</th><th>行业/基金名称</th><th>建议买入金额（元）</th><th>推荐理由</th>韶"
            for rec in recommendations:
                html += f"<tr><td>{rec['code']}</td><td>{rec['name']}</td><td>{rec['amount']}</td><td>{rec['reason']}</td></tr>"
            html += "</table>"
            total_invest = sum(r.get('amount',0) for r in recommendations)
            html += f"<p>💡 实际买入总金额: {total_invest} 元（占现金 {total_invest/cash*100:.1f}%）</p>"
        else:
            html += "<p>⚠️ 未能生成有效的行业轮动推荐，请检查数据源或稍后重试。</p>"
    else:
        html += "<h2>💰 当前持仓</h2><table><tr><th>代码</th><th>名称</th><th>份额</th><th>成本价</th><th>现价</th><th>浮动盈亏</th></tr>"
        for fund in holdings:
            profit = (fund.get('current',0) - fund.get('cost',0)) * fund.get('amount',0)
            html += f"<tr><td>{fund['code']}</td><td>{fund.get('name','-')}</td><td>{fund['amount']}</td><td>{fund['cost']:.4f}</td><td>{fund.get('current',0):.4f}</td><td>{profit:+.2f}</td></tr>"
        html += "</table><p>📌 有持仓时系统仅提供风控建议，不自动生成买卖指令。</p>"
    html += "</body></html>"
    return html

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["recommend", "hold"], default="recommend")
    args = parser.parse_args()
    raw_data = load_json(HOLDINGS_FILE, {"holdings": [], "cash": DEFAULT_CASH})
    if isinstance(raw_data, list):
        holdings_list = raw_data
        cash = DEFAULT_CASH
    else:
        holdings_list = raw_data.get("holdings", [])
        cash = raw_data.get("cash", DEFAULT_CASH)
    logger.info(f"加载持仓 {len(holdings_list)} 只基金，现金 {cash} 元")
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
    market_risk = get_market_risk_level()
    risk_advice = get_risk_advice(holdings_list, cash, market_risk)
    sources = load_json(SOURCES_FILE, [])
    if not sources:
        sources = [
            "https://feeds.bloomberg.com/markets/news.rss",
            "https://feeds.bloomberg.com/economics/news.rss",
            "http://feeds.bbci.co.uk/news/business/rss.xml",
            "https://www.wsj.com/xml/rss/3_7085.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        ]
    news = fetch_all_news(sources)
    events = []  # 情绪因子已废弃，传空列表
    recommendations = []
    sector_df = None
    concentration_warning = ""
    if args.mode == "recommend" and not holdings_list:
        logger.info("使用行业轮动多因子模型生成推荐...")
        sector_list = list(SECTORS.keys())
        try:
            sector_df = fetch_all_sector_data(sector_list, events)
            if sector_df is not None and not sector_df.empty:
                recommendations = allocate_weights_by_score(sector_df, cash, top_n=3, risk_level=market_risk.get("level","medium"))
                concentration_warning = generate_concentration_warning(recommendations)
                logger.info(f"行业轮动生成 {len(recommendations)} 条推荐")
            else:
                logger.warning("行业数据获取失败")
        except Exception as e:
            logger.error(f"行业轮动模块执行错误: {e}", exc_info=True)
    html = generate_html_report(holdings_list, news, sector_df, recommendations, market_risk, risk_advice, args.mode, cash, concentration_warning)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"报告已保存至 {REPORT_FILE}")
    if args.mode == "recommend" and recommendations:
        os.makedirs(os.path.dirname(RECOMMENDATIONS_FILE), exist_ok=True)
        existing = load_json(RECOMMENDATIONS_FILE, [])
        existing.append({"date": datetime.now().strftime("%Y-%m-%d"), "recommendations": recommendations, "market_risk": market_risk.get("level")})
        save_json(RECOMMENDATIONS_FILE, existing)
    version_data = {"last_updated": datetime.now().isoformat(), "version": int(datetime.now().timestamp())}
    with open("report_version.json", "w") as f:
        json.dump(version_data, f)
    logger.info("任务完成")

if __name__ == "__main__":
    main()
