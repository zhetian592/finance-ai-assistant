#!/usr/bin/env python3
# fund_crawler.py - 完整版：RSS抓取 + 量化数据 + AI分析 + 回测记录
import os
import json
import re
import time
import random
import logging
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

import requests
import feedparser
from bs4 import BeautifulSoup

# ================= 导入量化模块 =================
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
    logging.info("AKShare 量化模块加载成功")
except ImportError:
    AKSHARE_AVAILABLE = False
    logging.warning("AKShare 未安装，量化数据功能不可用")

from quant import get_market_risk_level, update_fund_nav, check_position_risk, get_risk_advice_for_ai
from backtest import record_ai_recommendation

# ================= 日志配置 =================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 环境变量 =================
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
GH_MODELS_TOKEN = os.environ.get("GH_MODELS_TOKEN")
ANALYSIS_MODE = os.environ.get("ANALYSIS_MODE", "recommend")  # recommend / hold

if not OPENROUTER_API_KEY:
    logger.error("请设置环境变量 OPENROUTER_API_KEY")
    sys.exit(1)

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
GITHUB_MODELS_BASE_URL = "https://models.inference.ai.azure.com"
REQUEST_TIMEOUT = 45
MAX_RETRIES = 2

# ================= RSS 源配置 =================
RSS_FEEDS = [
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://www.ft.com/?format=rss",
    "https://www.wsj.com/xml/rss/3_7085.xml",
]

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# ================= 辅助函数 =================
def clean_html(text: str) -> str:
    if not text:
        return ""
    if text.strip().startswith("<"):
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text().strip()[:500]
    return text[:500]

def parse_published(published_str: str) -> Optional[datetime]:
    if not published_str:
        return None
    formats = [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(published_str, fmt)
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt
        except:
            continue
    return None

def fetch_rss_feed(url: str) -> List[Dict]:
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"RSS 返回 {resp.status_code} - {url}")
            return []
        feed = feedparser.parse(resp.content)
        items = []
        cutoff = datetime.utcnow() - timedelta(hours=48)
        for entry in feed.entries[:20]:
            published = entry.get("published", entry.get("updated", ""))
            pub_dt = parse_published(published)
            if pub_dt and pub_dt < cutoff:
                continue
            title = clean_html(entry.get("title", ""))
            summary = clean_html(entry.get("summary", ""))
            if not summary:
                summary = title
            link = entry.get("link", "")
            items.append({
                "title": title,
                "link": link,
                "summary": summary[:500],
                "source": url
            })
        return items
    except Exception as e:
        logger.error(f"抓取失败 {url}: {e}")
        return []

def fetch_all_news() -> List[Dict]:
    logger.info(f"开始抓取 {len(RSS_FEEDS)} 个财经 RSS")
    all_items = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(fetch_rss_feed, url): url for url in RSS_FEEDS}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                items = future.result()
                all_items.extend(items)
                logger.info(f"✓ {url} -> {len(items)} 条")
            except Exception as e:
                logger.error(f"✗ {url} 异常: {e}")
    logger.info(f"共抓取 {len(all_items)} 条财经资讯")
    return all_items

def load_holdings() -> Dict:
    try:
        with open("holdings.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"读取 holdings.json 失败: {e}")
        return {"holdings": [], "cash": 0}

# ================= AI 调用 =================
def call_openrouter(prompt: str) -> Optional[str]:
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/zhetian592/finance-ai-assistant",
                    "X-Title": "Fund Recommender"
                },
                json={
                    "model": "openrouter/free",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 2000,
                },
                timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
            else:
                logger.warning(f"OpenRouter 尝试 {attempt+1}: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"OpenRouter 异常 (尝试 {attempt+1}): {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(3)
    return None

def call_github_model(prompt: str) -> Optional[str]:
    if not GH_MODELS_TOKEN:
        return None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                f"{GITHUB_MODELS_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {GH_MODELS_TOKEN}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 2000,
                },
                timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
            else:
                logger.warning(f"GitHub Models 尝试 {attempt+1}: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"GitHub Models 异常: {e}")
        if attempt < MAX_RETRIES - 1:
            time.sleep(3)
    return None

def parse_ai_output(content: str, valid_codes: set) -> List[Dict]:
    try:
        json_match = re.search(r'\[\s*\{.*?\}\s*\]', content, re.DOTALL)
        if not json_match:
            return []
        data = json.loads(json_match.group(0))
        filtered = []
        for item in data:
            code = item.get("fund_code")
            if code and code in valid_codes:
                if "evidence" not in item:
                    item["evidence"] = ["依据市场资讯分析"]
                filtered.append(item)
        return filtered
    except Exception as e:
        logger.warning(f"JSON 解析失败: {e}")
        return []

def multi_model_vote(prompt: str, valid_codes: set) -> Dict:
    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_open = executor.submit(call_openrouter, prompt)
        future_github = executor.submit(call_github_model, prompt)
        results["openrouter"] = future_open.result(timeout=60) if future_open else None
        results["github"] = future_github.result(timeout=60) if future_github else None

    fund_votes = {}
    for model_name, content in results.items():
        if not content:
            continue
        suggestions = parse_ai_output(content, valid_codes)
        logger.info(f"模型 {model_name} 解析出 {len(suggestions)} 条有效建议")
        for sug in suggestions:
            code = sug.get("fund_code")
            name = sug.get("fund_name", "")
            op = sug.get("recommendation", "").lower()
            if op not in ["buy","sell","hold","add"]:
                continue
            if code not in fund_votes:
                fund_votes[code] = {"name": name, "buy":0, "sell":0, "hold":0, "add":0, "evidences":[]}
            fund_votes[code][op] += 1
            fund_votes[code]["evidences"].extend(sug.get("evidence", []))

    final = []
    for code, votes in fund_votes.items():
        max_op = max(["buy","sell","hold","add"], key=lambda k: votes[k])
        final.append({
            "fund_code": code,
            "fund_name": votes["name"],
            "recommendation": max_op,
            "votes": {k: votes[k] for k in ["buy","sell","hold","add"]},
            "evidences": list(set(votes["evidences"]))[:3]
        })
    return {"final_recommendations": final, "models_participated": sum(1 for v in results.values() if v)}

# ================= 报告生成 =================
def generate_html_report(aggregated: Dict, news_count: int, holdings_exist: bool, mode: str,
                         market_risk=None, risk_advice=None) -> str:
    if not holdings_exist:
        return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>基金报告</title></head>
<body><h1>📈 基金报告</h1><p>暂无持仓，请先在仪表盘添加基金。</p>
<p>生成时间：{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p></body></html>"""

    rows = ""
    for rec in aggregated["final_recommendations"]:
        op_cn = {"buy":"买入", "sell":"卖出", "hold":"持有", "add":"加仓"}.get(rec["recommendation"], rec["recommendation"])
        evidence = rec["evidences"][0] if rec["evidences"] else "无"
        rows += f"<tr><td>{rec['fund_code']}</td><td>{rec['fund_name']}</td><td>{op_cn}</td><td>{evidence}</td></tr>"

    # 构建量化数据部分
    quant_section = ""
    if market_risk:
        quant_section = f"""
        <div style="background:#f0f4f8; padding:12px; border-radius:8px; margin-bottom:20px;">
            <h3>📊 市场量化数据</h3>
            <p><strong>市场风险等级</strong>: {market_risk.get('level', 'unknown')}</p>
            <p><strong>风险评分</strong>: {market_risk.get('score', 0)}</p>
            <p><strong>风险建议</strong>: {market_risk.get('advice', '')}</p>
            <p><strong>沪深300 PE</strong>: {market_risk.get('valuation', {}).get('current_pe', 'N/A')}</p>
            <p><strong>北向资金净流入(近5日)</strong>: {market_risk.get('north_flow', {}).get('total_net_billion', 'N/A')} 亿元</p>
        </div>
        """
    if risk_advice:
        quant_section += f"""
        <div style="background:#fff3cd; padding:12px; border-radius:8px; margin-bottom:20px;">
            <h3>⚠️ 风控提示</h3>
            <pre style="white-space:pre-wrap; margin:0;">{risk_advice}</pre>
        </div>
        """

    title = "基金推荐报告" if mode == "recommend" else "持有分析报告"
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{title}</title>
<style>
    body{{font-family:Arial;margin:20px}}
    table{{border-collapse:collapse;width:100%;margin-top:20px}}
    th,td{{border:1px solid #ddd;padding:8px;text-align:left}}
    th{{background:#f2f2f2}}
    .quant-box{{background:#f0f4f8;padding:12px;border-radius:8px;margin-bottom:20px}}
    .risk-box{{background:#fff3cd;padding:12px;border-radius:8px;margin-bottom:20px}}
</style>
</head>
<body>
<h1>📈 {title}</h1>
<p>生成时间：{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
<p>参与模型数：{aggregated['models_participated']}</p>
<p>抓取资讯数：{news_count}</p>
{quant_section}
<h2>分析建议</h2>
<table><thead><tr><th>代码</th><th>名称</th><th>操作</th><th>依据证据</th></tr></thead><tbody>{rows}</tbody></table>
<p>注：仅供参考，不构成投资建议。</p>
</body></html>"""
    return html

def save_report(content: str):
    with open("fund_report.html", "w", encoding="utf-8") as f:
        f.write(content)
    logger.info("报告已保存")

# ================= 主流程 =================
def main():
    # 1. 加载持仓
    holdings = load_holdings()
    holdings_list = holdings.get("holdings", [])
    valid_codes = {h["fund_code"] for h in holdings_list}

    # 2. 获取市场量化数据
    market_risk = get_market_risk_level() if AKSHARE_AVAILABLE else None
    risk_advice = get_risk_advice_for_ai(holdings_list, holdings.get("cash", 0), market_risk) if AKSHARE_AVAILABLE else None

    # 3. 自动更新基金净值
    if AKSHARE_AVAILABLE and holdings_list:
        updated = update_fund_nav(holdings_list)
        if updated:
            logger.info("基金净值已自动更新")

    if not holdings_list:
        logger.info("无持仓，生成空报告")
        html = generate_html_report({}, 0, False, ANALYSIS_MODE, market_risk, risk_advice)
        save_report(html)
        return

    # 4. 构建持仓文本（包含盈亏信息）
    holdings_text = json.dumps(holdings, ensure_ascii=False, indent=2)

    # 5. 抓取新闻
    news = fetch_all_news()
    news_summary = "\n".join([f"- {a['title']} ({a['source']})" for a in news[:50]]) if news else "无资讯"

    # 6. 构建量化数据文本
    quant_text = ""
    if market_risk:
        quant_text = f"""
**市场量化数据**：
- 沪深300 PE: {market_risk.get('valuation', {}).get('current_pe', 'N/A')}
- 北向资金近5日净流入: {market_risk.get('north_flow', {}).get('total_net_billion', 'N/A')} 亿元
- 市场风险等级: {market_risk.get('level', 'unknown')}
- 市场建议: {market_risk.get('advice', '')}
"""

    # 7. 构建提示词
    if ANALYSIS_MODE == "hold":
        prompt = f"""你是一名投资分析师。用户已持有以下基金，请根据最新市场资讯和量化数据，分析每只基金是否应该继续持有、卖出或加仓。必须引用新闻原文作为证据。

持仓及盈亏：
{holdings_text}
{quant_text}
近期市场资讯：
{news_summary}

输出JSON数组，每个元素包含：
fund_code, fund_name, recommendation(hold/sell/add), evidence(引用新闻中的具体语句), reason(简短理由)。

请分析："""
    else:
        prompt = f"""你是一名投资顾问。根据以下市场资讯、量化数据和用户持仓，对每只基金给出操作建议(buy/sell/hold/add)。必须引用新闻原文作为证据。

持仓：
{holdings_text}
{quant_text}
市场资讯：
{news_summary}

输出JSON数组，每个元素包含：
fund_code, fund_name, recommendation(buy/sell/hold/add), evidence(引用新闻中的具体语句), reason(简短理由)。

请分析："""

    # 8. 多模型投票
    aggregated = multi_model_vote(prompt, valid_codes)

    # 9. 记录AI建议（用于回测）
    for rec in aggregated["final_recommendations"]:
        # 获取当前净值作为记录
        nav = None
        for h in holdings_list:
            if h.get("fund_code") == rec["fund_code"]:
                nav = h.get("current_nav")
                break
        record_ai_recommendation(
            fund_code=rec["fund_code"],
            fund_name=rec["fund_name"],
            action=rec["recommendation"],
            reason=rec["evidences"][0] if rec["evidences"] else "",
            nav_at_time=nav,
            amount_suggested=0
        )

    # 10. 生成报告
    html = generate_html_report(aggregated, len(news), True, ANALYSIS_MODE, market_risk, risk_advice)
    save_report(html)
    logger.info("完成")

if __name__ == "__main__":
    main()
