#!/usr/bin/env python3
"""
财经AI决策辅助工具 - 行业轮动版（最终优化版）
- 抓取 RSS 新闻 → 提取结构化事件（主题+情感）
- 获取申万一级行业的多因子数据（估值、资金流、动量、情绪）
- 因子缺失时显示 N/A，综合得分仅计算有效因子
- 引入估值陷阱折价（估值>80且动量<55时折价15%）
- 仓位与市场风险等级挂钩（中等风险仓位上限50%）
- 自动检测组合风格集中度并发出警告
- 有持仓时：仅展示持仓盈亏和市场风控
- 报告中去除了“近期财经新闻”栏目
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

# 量化模块
from quant import get_market_risk_level, update_fund_nav, get_risk_advice

# 事件提取（FinBERT + 关键词）
from event_extractor import extract_event

# 行业轮动模块
from data_fetcher import fetch_all_sector_data, SECTORS
from sector_rotator import allocate_weights_by_score, generate_sector_report, generate_concentration_warning

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

DEFAULT_CASH = 50000  # 默认现金

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

# ==================== 新闻抓取 ====================
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
    # 按标题去重
    seen = set()
    unique = []
    for item in all_news:
        title = item.get("title", "")
        if title and title not in seen:
            seen.add(title)
            unique.append(item)
    return unique

# ==================== 报告生成 ====================
def generate_html_report(holdings: List[Dict], news: List[Dict],
                         sector_df: Any, recommendations: List[Dict],
                         market_risk: Dict, risk_advice: str,
                         mode: str, cash: float, concentration_warning: str = "") -> str:
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
        h2, h3 {{ color: #34495e; }}
        .risk {{ padding: 10px; border-radius: 5px; margin: 10px 0; }}
        .risk-high {{ background-color: #ffebee; border-left: 5px solid red; }}
        .risk-medium {{ background-color: #fff3e0; border-left: 5px solid orange; }}
        .risk-low {{ background-color: #e8f5e9; border-left: 5px solid green; }}
        table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .news-item {{ margin-bottom: 15px; padding: 10px; background: #f9f9f9; }}
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
        # 无持仓：显示行业轮动推荐
        if sector_df is not None and not sector_df.empty:
            html += generate_sector_report(sector_df, top_n=8)
        if recommendations:
            html += "<h2>🌟 行业轮动基金推荐</h2>"
            if concentration_warning:
                html += f'<div class="warning">{concentration_warning}</div>'
            html += "<tr><th>基金代码</th><th>行业/基金名称</th><th>建议买入金额（元）</th><th>推荐理由</th></tr>"
            for rec in recommendations:
                code = rec.get('code', '')
                name = rec.get('name', '')
                amount = rec.get('amount', 0)
                reason = rec.get('reason', '')
                html += f"<tr><td>{code}</td><td>{name}</td><td>{amount}</td><td>{reason}</td></tr>"
            html += "</table>"
            total_invest = sum(r.get('amount',0) for r in recommendations)
            html += f"<p>💡 实际买入总金额: {total_invest} 元（占现金 {total_invest/cash*100:.1f}%），剩余现金保留为防御仓位。</p>"
        else:
            html += "<p>⚠️ 未能生成有效的行业轮动推荐，请检查数据源或稍后重试。</p>"
    else:
        # 有持仓：显示持仓盈亏表
        html += "<h2>💰 当前持仓</h2>"
        html += "<table><tr><th>基金代码</th><th>名称</th><th>持有份额</th><th>成本价</th><th>现价</th><th>浮动盈亏</th></tr>"
        for fund in holdings:
            code = fund.get('code', '')
            name = fund.get('name', '')
            amount = fund.get('amount', 0)
            cost = fund.get('cost', 0)
            current = fund.get('current', 0)
            profit = (current - cost) * amount
            profit_class = "color: green" if profit >= 0 else "color: red"
            html += f"<tr><td>{code}</td><td>{name}</td><td>{amount}</td><td>{cost:.4f}</td><td>{current:.4f}</td><td style='{profit_class}'>{profit:+.2f}</td></tr>"
        html += "</table>"
        html += "<p>📌 注：有持仓时系统仅提供市场风控建议，不自动生成买卖指令。</p>"

    # 已移除“近期财经新闻”模块（2025-04-24）
    html += """
</body>
</html>
"""
    return html

# ==================== 主函数 ====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["recommend", "hold"], default="recommend",
                        help="recommend: 无持仓时行业轮动推荐; hold: 仅更新数据不推荐")
    args = parser.parse_args()

    # 1. 加载持仓
    raw_data = load_json(HOLDINGS_FILE, {"holdings": [], "cash": DEFAULT_CASH})
    if isinstance(raw_data, list):
        holdings_list = raw_data
        cash = DEFAULT_CASH
    else:
        holdings_list = raw_data.get("holdings", [])
        cash = raw_data.get("cash", DEFAULT_CASH)
    logger.info(f"加载持仓 {len(holdings_list)} 只基金，现金 {cash} 元")

    # 2. 更新基金净值（如果有持仓）
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

    # 3. 获取市场风险等级
    market_risk = get_market_risk_level()
    logger.info(f"市场风险等级: {market_risk.get('level')}")

    # 4. 风控建议
    risk_advice = get_risk_advice(holdings_list, cash, market_risk)

    # 5. 抓取新闻（仅用于情绪因子计算，不在报告中显示）
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

    # 6. 提取新闻事件（用于情绪得分）
    events = []
    for item in news[:20]:
        evt = extract_event(item)
        if evt["topics"]:
            events.append(evt)
    logger.info(f"提取到 {len(events)} 个结构化事件（含主题）")

    # 7. 行业轮动推荐（仅当无持仓且模式为 recommend）
    recommendations = []
    sector_df = None
    concentration_warning = ""
    if args.mode == "recommend" and not holdings_list:
        logger.info("使用行业轮动多因子模型生成推荐...")
        sector_list = list(SECTORS.keys())
        try:
            sector_df = fetch_all_sector_data(sector_list, events)
            if sector_df is not None and not sector_df.empty:
                risk_level = market_risk.get("level", "medium")
                recommendations = allocate_weights_by_score(sector_df, cash, top_n=3, risk_level=risk_level)
                logger.info(f"行业轮动生成 {len(recommendations)} 条推荐")
                concentration_warning = generate_concentration_warning(recommendations)
            else:
                logger.warning("行业数据获取失败，无法生成推荐")
        except Exception as e:
            logger.error(f"行业轮动模块执行错误: {e}", exc_info=True)
    else:
        logger.info("跳过行业轮动推荐（有持仓或非recommend模式）")

    # 8. 生成 HTML 报告
    html = generate_html_report(
        holdings=holdings_list,
        news=news,
        sector_df=sector_df,
        recommendations=recommendations,
        market_risk=market_risk,
        risk_advice=risk_advice,
        mode=args.mode,
        cash=cash,
        concentration_warning=concentration_warning
    )
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"报告已保存至 {REPORT_FILE}")

    # 9. 保存回测记录（只保存行业轮动推荐）
    if args.mode == "recommend" and recommendations:
        os.makedirs(os.path.dirname(RECOMMENDATIONS_FILE), exist_ok=True)
        existing = load_json(RECOMMENDATIONS_FILE, [])
        new_record = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "recommendations": recommendations,
            "market_risk": market_risk.get("level"),
            "events_count": len(events)
        }
        existing.append(new_record)
        save_json(RECOMMENDATIONS_FILE, existing)
        logger.info(f"回测记录已追加至 {RECOMMENDATIONS_FILE}")
    else:
        if not os.path.exists(RECOMMENDATIONS_FILE):
            save_json(RECOMMENDATIONS_FILE, [])
            logger.info("创建空回测记录")

    # 10. 生成版本文件（用于前端检测更新）
    version_data = {
        "last_updated": datetime.now().isoformat(),
        "version": int(datetime.now().timestamp())
    }
    with open("report_version.json", "w", encoding='utf-8') as f:
        json.dump(version_data, f, indent=2)
    logger.info("版本信息已保存至 report_version.json")

    logger.info("任务完成")

if __name__ == "__main__":
    main()
