#!/usr/bin/env python3
# fund_crawler.py - 双模型投票 + 详细错误日志
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

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 读取环境变量
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
GH_MODELS_TOKEN = os.environ.get("GH_MODELS_TOKEN")

logger.info(f"OPENROUTER_API_KEY 是否设置: {bool(OPENROUTER_API_KEY)}")
logger.info(f"GH_MODELS_TOKEN 是否设置: {bool(GH_MODELS_TOKEN)}")

if not OPENROUTER_API_KEY:
    logger.error("请设置环境变量 OPENROUTER_API_KEY")
    sys.exit(1)

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
GITHUB_MODELS_BASE_URL = "https://models.inference.ai.azure.com"
REQUEST_TIMEOUT = 45
MAX_RETRIES = 2

RSS_FEEDS = [
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://www.ft.com/?format=rss",
    "https://www.wsj.com/xml/rss/3_7085.xml",
]

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

# ---------- 抓取 RSS ----------
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

# ---------- AI 模型调用 ----------
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
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3)
        except Exception as e:
            logger.warning(f"OpenRouter 异常 (尝试 {attempt+1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
    return None

def call_github_model(prompt: str) -> Optional[str]:
    if not GH_MODELS_TOKEN:
        logger.warning("GH_MODELS_TOKEN 未设置，跳过 GitHub Models")
        return None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                f"{GITHUB_MODELS_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {GH_MODELS_TOKEN}",
                    "Content-Type": "application/json",
                },
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
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3)
        except Exception as e:
            logger.warning(f"GitHub Models 异常 (尝试 {attempt+1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
    return None

def parse_json_output(content: str) -> List[Dict]:
    try:
        json_match = re.search(r'\[\s*\{.*?\}\s*\]', content, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0))
        else:
            return []
    except Exception as e:
        logger.warning(f"JSON 解析失败: {e}")
        return []

# ---------- 投票聚合 ----------
def multi_model_vote(prompt: str) -> Dict:
    logger.info("开始多模型投票...")
    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_open = executor.submit(call_openrouter, prompt)
        future_github = executor.submit(call_github_model, prompt)
        results["openrouter"] = future_open.result()
        results["github"] = future_github.result()

    fund_votes = {}
    for model_name, content in results.items():
        if not content:
            continue
        suggestions = parse_json_output(content)
        for sug in suggestions:
            code = sug.get("fund_code")
            op = sug.get("recommendation", "").lower()
            if not code or op not in ["buy","sell","hold","adjust"]:
                continue
            if code not in fund_votes:
                fund_votes[code] = {"buy":0,"sell":0,"hold":0,"adjust":0,"reasons":[]}
            fund_votes[code][op] += 1
            fund_votes[code]["reasons"].append({
                "model": model_name,
                "reason": sug.get("reason",""),
                "amount": sug.get("suggested_amount",0)
            })

    final = []
    for code, votes in fund_votes.items():
        max_op = max(votes, key=lambda k: votes[k] if k in ["buy","sell","hold","adjust"] else -1)
        final.append({
            "fund_code": code,
            "recommendation": max_op,
            "votes": {k: votes[k] for k in ["buy","sell","hold","adjust"]},
            "sample_reasons": votes["reasons"][:2]
        })
    return {"final_recommendations": final, "models_participated": sum(1 for v in results.values() if v)}

# ---------- 报告生成 ----------
def generate_html_report(aggregated: Dict, news_count: int) -> str:
    rows = ""
    for rec in aggregated["final_recommendations"]:
        op_cn = {"buy":"买入","sell":"卖出","hold":"持有","adjust":"调仓"}.get(rec["recommendation"], rec["recommendation"])
        reason = rec["sample_reasons"][0]["reason"] if rec["sample_reasons"] else "无"
        rows += f"<tr><td>{rec['fund_code']}</td><td>{op_cn}</td><td>{reason}</td></tr>"
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>基金推荐报告</title>
<style>body{{font-family:Arial;margin:20px}} table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #ddd;padding:8px}} th{{background:#f2f2f2}}</style>
</head>
<body>
<h1>📈 基金推荐报告</h1>
<p>生成时间：{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
<p>参与模型数：{aggregated['models_participated']}</p>
<p>抓取资讯数：{news_count}</p>
<table><thead><tr><th>基金代码</th><th>操作</th><th>理由</th></tr></thead><tbody>{rows}</tbody></table>
<p>注：仅供参考，不构成投资建议。</p>
</body></html>"""
    return html

def save_report(content: str):
    with open("fund_report.html", "w", encoding="utf-8") as f:
        f.write(content)
    logger.info("报告已保存")

def main():
    news = fetch_all_news()
    news_summary = "\n".join([f"- {a['title']} ({a['source']})" for a in news[:50]]) if news else "无资讯"
    holdings = load_holdings()
    holdings_text = json.dumps(holdings, ensure_ascii=False, indent=2)

    prompt = f"""你是一名投资顾问。根据资讯和持仓，对每只基金给出操作建议（buy/sell/hold/adjust）。

持仓：
{holdings_text}

资讯：
{news_summary}

输出JSON数组，每个元素包含：fund_code, recommendation, suggested_amount, reason（30字内）。
示例：[{{"fund_code":"000001","recommendation":"hold","suggested_amount":0,"reason":"市场震荡"}}]

请输出："""

    aggregated = multi_model_vote(prompt)
    html = generate_html_report(aggregated, len(news))
    save_report(html)
    logger.info("完成")

if __name__ == "__main__":
    main()
