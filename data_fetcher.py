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

# 行业轮动模块
from data_fetcher import fetch_all_sector_data, SECTORS
from sector_rotator import allocate_weights_by_score, generate_sector_report, generate_concentration_warning
# 注意：已移除 event_extractor，改用内联简化版情绪分析
from data_services import DataService

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

_ds = DataService()

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

# ==================== 简化版事件提取（无需单独文件） ====================
def extract_event_simple(news_item: dict) -> dict:
    """简易版事件提取，使用 data_services 中的情感分析"""
    title = news_item.get("title", "")
    summary = news_item.get("summary", "")
    full_text = title + ". " + summary
    sentiment = _ds.analyze_sentiment(full_text)
    # 简单主题提取：基于关键词匹配（示例）
    topics = []
    sector_keywords = {
        "建筑材料": ["水泥", "玻璃", "建材", "基建"],
        "银行": ["银行", "降息", "信贷", "股息"],
        "非银金融": ["券商", "保险", "证券"],
        "电子": ["芯片", "半导体", "AI", "消费电子"],
        "食品饮料": ["白酒", "食品", "饮料"],
        "医药生物": ["医药", "生物", "疫苗"],
    }
    for sector, kws in sector_keywords.items():
        if any(kw in full_text for kw in kws):
            topics.append(sector)
    return {
        "title": title,
        "summary": summary[:200],
        "topics": topics,
        "sentiment": sentiment["label"],
        "sentiment_score": sentiment["score"],
        "url": news_item.get("link", ""),
        "timestamp": news_item.get("published", "")
    }

# ==================== 报告生成 ====================
def generate_html_report(holdings: List[Dict], news: List[Dict],
                         sector_df: Any, recommendations: List[Dict],
                         market_risk: Dict, risk_advice: str,
                         mode: str, cash: float, concentration_warning: str = "") -> str:
    # 此函数内容不变，保持之前的版本（已移除新闻列表）
    # 为了节省篇幅，此处省略，但你应该保留之前正确的 generate_html_report
    # 实际使用时请从之前的代码中复制完整函数
    # 由于你可能已经有这个函数，我这里只写占位，确保语法正确
    return "<html>...</html>"

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

    # 5. 抓取新闻
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

    # 6. 提取新闻事件（用于情绪得分）使用简化版函数
    events = []
    for item in news[:20]:
        evt = extract_event_simple(item)
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

    # 8. 生成 HTML 报告（这里需要调用完整的 generate_html_report，请确保函数存在）
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

    # 9. 保存回测记录
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

    # 10. 生成版本文件
    version_data = {
        "last_updated": datetime.now().isoformat(),
        "version": int(datetime.now().timestamp())
    }
    with open("report_version.json", "w", encoding='utf-8') as f:
        json.dump(version_data, f)
    logger.info("版本信息已保存至 report_version.json")

    logger.info("任务完成")

if __name__ == "__main__":
    main()
