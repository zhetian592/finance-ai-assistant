#!/usr/bin/env python3
# fund_crawler.py - 双模型稳定投票（OpenRouter + GitHub Models）
import os
import json
import re
import time
import random
import logging
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple, Optional

import requests
import feedparser
from bs4 import BeautifulSoup

# ================= 日志配置 =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ================= 配置 =================
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
GH_MODELS_TOKEN = os.environ.get("GH_MODELS_TOKEN") or os.environ.get("GITHUB_TOKEN")

if not OPENROUTER_API_KEY:
    logger.error("请设置环境变量 OPENROUTER_API_KEY")
    sys.exit(1)
if not GH_MODELS_TOKEN:
    logger.warning("未设置 GH_MODELS_TOKEN，GitHub Models 将不可用")

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
GITHUB_MODELS_BASE_URL = "https://models.inference.ai.azure.com"

# 稳定模型列表
MODELS = [
    ("openrouter", "openrouter/free"),           # OpenRouter 自动路由
    ("github", "gpt-4o-mini"),                  # GitHub Models (需 GH_MODELS_TOKEN)
]

MAX_WORKERS = 2
REQUEST_TIMEOUT = 30

# 官方财经 RSS 源（稳定）
RSS_FEEDS = [
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://feeds.bloomberg.com/markets/news.rss",
    "https://www.ft.com/?format=rss",
    "https://www.wsj.com/xml/rss/3_7085.xml",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
]

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
                "published": published,
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

# ================= AI 模型调用 =================
def call_openrouter_model(model: str, prompt: str) -> Tuple[str, Optional[str]]:
    for attempt in range(2):
        try:
            response = requests.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/zhetian592/finance-ai-assistant",
                    "X-Title": "Fund Recommender"
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 2000,
                },
                timeout=REQUEST_TIMEOUT
            )
            if response.status_code == 200:
                return (model, response.json()["choices"][0]["message"]["content"])
            else:
                logger.warning(f"OpenRouter 模型 {model} 返回 {response.status_code} (尝试 {attempt+1}/2)")
                if attempt == 0:
                    time.sleep(2)
        except Exception as e:
            logger.warning(f"OpenRouter 模型 {model} 调用异常 (尝试 {attempt+1}/2): {e}")
            if attempt == 0:
                time.sleep(2)
    return (model, None)

def call_github_model(model: str, prompt: str) -> Tuple[str, Optional[str]]:
    if not GH_MODELS_TOKEN:
        return (model, None)
    for attempt in range(2):
        try:
            response = requests.post(
                f"{GITHUB_MODELS_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {GH_MODELS_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 2000,
                },
                timeout=REQUEST_TIMEOUT
            )
            if response.status_code == 200:
                return (model, response.json()["choices"][0]["message"]["content"])
            else:
                logger.warning(f"GitHub 模型 {model} 返回 {response.status_code} (尝试 {attempt+1}/2)")
                if attempt == 0:
                    time.sleep(2)
        except Exception as e:
            logger.warning(f"GitHub 模型 {model} 调用异常 (尝试 {attempt+1}/2): {e}")
            if attempt == 0:
                time.sleep(2)
    return (model, None)

def multi_model_vote(prompt: str) -> Dict[str, Any]:
    logger.info(f"开始多模型投票，模型列表: {MODELS}")
    results = {}
    with ThreadPoolExecutor(max_workers=len(MODELS)) as executor:
        future_to_model = {}
        for provider, model in MODELS:
            if provider == "openrouter":
                future = executor.submit(call_openrouter_model, model, prompt)
            else:
                future = executor.submit(call_github_model, model, prompt)
            future_to_model[future] = f"{provider}:{model}"
        for future in as_completed(future_to_model):
            model_name, content = future.result()
            results[model_name] = content
            if content:
                logger.info(f"模型 {model_name} 返回成功")
            else:
                logger.warning(f"模型 {model_name} 返回失败")

    # 解析投票
    fund_votes = {}
    for model_name, content in results.items():
        if not content:
            continue
        try:
            json_match = re.search(r'\[\s*\{.*?\}\s*\]', content, re.DOTALL)
            if not json_match:
                logger.warning(f"模型 {model_name} 输出中未找到 JSON 数组")
                continue
            suggestions = json.loads(json_match.group(0))
        except Exception as e:
            logger.warning(f"解析模型 {model_name} 输出失败: {e}")
            continue

        for sug in suggestions:
            code = sug.get("fund_code", "")
            op = sug.get("recommendation", "").lower()
            if not code or op not in ["buy", "sell", "hold", "adjust"]:
                continue
            if code not in fund_votes:
                fund_votes[code] = {"buy": 0, "sell": 0, "hold": 0, "adjust": 0, "reasons": []}
            fund_votes[code][op] += 1
            fund_votes[code]["reasons"].append({
                "model": model_name,
                "reason": sug.get("reason", ""),
                "amount": sug.get("suggested_amount", 0)
            })

    final_recommendations = []
    for code, votes in fund_votes.items():
        max_op = max(votes, key=lambda k: votes[k] if k in ["buy","sell","hold","adjust"] else -1)
        final_recommendations.append({
            "fund_code": code,
            "recommendation": max_op,
            "votes": {k: votes[k] for k in ["buy","sell","hold","adjust"]},
            "sample_reasons": votes["reasons"][:3]
        })
    return {
        "final_recommendations": final_recommendations,
        "models_participated": len([c for c in results.values() if c is not None])
    }

# ================= 报告生成 =================
def generate_html_report(aggregated: Dict, holdings: Dict, news_count: int) -> str:
    rows = ""
    for rec in aggregated["final_recommendations"]:
        op_cn = {"buy":"买入", "sell":"卖出", "hold":"持有", "adjust":"调仓"}.get(rec["recommendation"], rec["recommendation"])
        votes_str = f"买:{rec['votes']['buy']} 卖:{rec['votes']['sell']} 持:{rec['votes']['hold']} 调:{rec['votes']['adjust']}"
        reason_sample = rec["sample_reasons"][0]["reason"] if rec["sample_reasons"] else "无"
        rows += f"""
        <tr>
            <td>{rec['fund_code']}</td>
            <td>{op_cn}</td>
            <td>{votes_str}</td>
            <td>{reason_sample}</td>
        </tr>
        """
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>基金推荐报告</title>
<style>
    body {{ font-family: Arial, sans-serif; margin: 20px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
    th {{ background-color: #f2f2f2; }}
    .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
</style>
</head>
<body>
<h1>📈 基金推荐报告</h1>
<p>生成时间：{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
<p>参与投票模型数：{aggregated['models_participated']}</p>
<p>抓取财经资讯条数：{news_count}</p>
<h2>持仓分析建议</h2>
<table>
    <thead><tr><th>基金代码</th><th>推荐操作</th><th>投票分布</th><th>示例理由</th></tr></thead>
    <tbody>{rows}</tbody>
</table>
<div class="footer">
    <p>注：本报告由多AI模型投票生成，仅供参考，不构成投资建议。</p>
</div>
</body>
</html>"""
    return html

def save_report(html_content: str):
    with open("fund_report.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    logger.info("报告已保存: fund_report.html")

# ================= 主流程 =================
def main():
    # 1. 抓取财经资讯
    news_articles = fetch_all_news()
    if not news_articles:
        logger.warning("未抓取到任何财经资讯")
        news_summary = "无近期市场资讯。"
    else:
        news_summary = "\n".join([f"- {a['title']} ({a['source']})" for a in news_articles[:50]])

    # 2. 读取持仓
    holdings = load_holdings()
    holdings_text = json.dumps(holdings, ensure_ascii=False, indent=2)

    # 3. 构建提示词
    prompt = f"""你是一名专业的投资顾问。根据以下市场资讯和用户持仓，对**每只基金**分别给出操作建议。

**用户持仓**：
{holdings_text}

**近期市场资讯摘要**：
{news_summary}

**输出要求**：
请严格按照以下 JSON 格式输出，必须包含所有基金，不要遗漏：
[
  {{
    "fund_code": "基金代码",
    "fund_name": "基金名称",
    "recommendation": "buy/sell/hold/adjust",
    "suggested_amount": 数字（买入或调仓时的建议金额）,
    "reason": "决策理由（不超过30字）"
  }}
]

请分析："""

    # 4. 多模型投票
    aggregated = multi_model_vote(prompt)

    # 5. 生成并保存报告
    html = generate_html_report(aggregated, holdings, len(news_articles))
    save_report(html)

    # 6. 输出摘要到日志
    logger.info("=== 投票结果摘要 ===")
    for rec in aggregated["final_recommendations"]:
        logger.info(f"{rec['fund_code']} -> {rec['recommendation']} (买:{rec['votes']['buy']} 卖:{rec['votes']['sell']} 持:{rec['votes']['hold']})")

if __name__ == "__main__":
    main()
