#!/usr/bin/env python3
# fund_crawler.py - 财经RSS抓取 + 多模型投票基金推荐
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
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
# 多模型投票列表（免费模型）
MODELS = [
    "openrouter/free",                 # 自动路由到可用免费模型
    "qwen/qwen-7b-chat:free",          # 阿里通义千问
    "deepseek/deepseek-chat:free",     # DeepSeek
    "google/gemini-2.0-flash-exp:free",# Gemini
    "microsoft/phi-3-medium-128k-instruct:free"
]

# 请求配置
MAX_WORKERS = 5
REQUEST_TIMEOUT = 15
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
]

# ================= 加载配置 =================
def load_sources() -> List[str]:
    with open("fund_sources.json", "r", encoding="utf-8") as f:
        return json.load(f)

def load_holdings() -> Dict:
    with open("holdings.json", "r", encoding="utf-8") as f:
        return json.load(f)

RAW_SOURCES = load_sources()

# ================= RSS 抓取 =================
def clean_html(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text().strip()[:500]

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

def fetch_rss(url: str) -> List[Dict]:
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.debug(f"HTTP {resp.status_code} - {url}")
            return []
        feed = feedparser.parse(resp.content)
        items = []
        cutoff = datetime.utcnow() - timedelta(hours=48)  # 抓取最近48小时
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

def fetch_all_sources() -> List[Dict]:
    logger.info(f"开始抓取 {len(RAW_SOURCES)} 个财经信源")
    all_items = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(fetch_rss, url): url for url in RAW_SOURCES}
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

# ================= 多模型投票 =================
def call_model(model: str, prompt: str) -> Tuple[str, Optional[str]]:
    """调用单个模型，返回 (model_name, response_text)"""
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
            timeout=60
        )
        if response.status_code == 200:
            content = response.json()["choices"][0]["message"]["content"]
            return (model, content)
        else:
            logger.warning(f"模型 {model} 返回 {response.status_code}")
            return (model, None)
    except Exception as e:
        logger.warning(f"模型 {model} 调用失败: {e}")
        return (model, None)

def multi_model_vote(prompt: str, models: List[str]) -> Dict[str, Any]:
    """并发调用多个模型，返回投票结果"""
    logger.info(f"开始多模型投票，共 {len(models)} 个模型")
    results = {}
    with ThreadPoolExecutor(max_workers=len(models)) as executor:
        future_to_model = {executor.submit(call_model, m, prompt): m for m in models}
        for future in as_completed(future_to_model):
            model, content = future.result()
            results[model] = content
    return results

def parse_model_output(content: str) -> List[Dict]:
    """解析模型返回的 JSON 格式建议"""
    try:
        # 提取 JSON 部分
        json_match = re.search(r'\[\s*\{.*?\}\s*\]', content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(0))
            return data
        else:
            return []
    except Exception as e:
        logger.warning(f"解析模型输出失败: {e}")
        return []

def aggregate_votes(all_results: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """聚合投票结果，统计每只基金的操作建议"""
    # 统计每只基金的推荐操作
    fund_votes = {}  # {fund_code: {"buy":0, "sell":0, "hold":0, "adjust":0}}
    all_suggestions = []
    for model, content in all_results.items():
        if not content:
            continue
        suggestions = parse_model_output(content)
        for sug in suggestions:
            code = sug.get("fund_code", "")
            op = sug.get("recommendation", "").lower()
            if not code or op not in ["buy", "sell", "hold", "adjust"]:
                continue
            if code not in fund_votes:
                fund_votes[code] = {"buy": 0, "sell": 0, "hold": 0, "adjust": 0, "reasons": []}
            fund_votes[code][op] += 1
            fund_votes[code]["reasons"].append({
                "model": model,
                "reason": sug.get("reason", ""),
                "amount": sug.get("suggested_amount", 0)
            })
            all_suggestions.append(sug)
    # 计算每个基金的最终推荐（投票最多的操作）
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
        "all_suggestions": all_suggestions,
        "models_participated": len([c for c in all_results.values() if c is not None])
    }

# ================= 报告生成 =================
def generate_html_report(aggregated: Dict, holdings: Dict, news_count: int) -> str:
    """生成 HTML 格式的基金推荐报告"""
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

def save_report(html_content: str, md_content: str = None):
    with open("fund_report.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    if md_content:
        with open("fund_report.md", "w", encoding="utf-8") as f:
            f.write(md_content)
    logger.info("报告已保存: fund_report.html")

# ================= 主流程 =================
def main():
    if not OPENROUTER_API_KEY:
        logger.error("请设置环境变量 OPENROUTER_API_KEY")
        return

    # 1. 抓取财经资讯
    articles = fetch_all_sources()
    if not articles:
        logger.warning("未抓取到任何财经资讯")
        return

    # 2. 读取持仓
    holdings = load_holdings()
    holdings_text = json.dumps(holdings, ensure_ascii=False, indent=2)

    # 3. 构造提示词
    news_summary = "\n".join([f"- {a['title']} ({a['source']})" for a in articles[:50]])  # 限制条数
    prompt = f"""你是一名专业的投资顾问。根据以下市场资讯和用户持仓，对每只基金给出操作建议。

**用户持仓**：
{holdings_text}

**近期市场资讯摘要**：
{news_summary}

**输出要求**：
请严格按照以下 JSON 格式输出，不要添加任何额外解释：
[
  {{
    "fund_code": "基金代码",
    "fund_name": "基金名称",
    "recommendation": "buy/sell/hold/adjust",
    "suggested_amount": 数字（买入或调仓时填写）,
    "reason": "决策理由（不超过30字）"
  }}
]

请分析："""

    # 4. 多模型投票
    results = multi_model_vote(prompt, MODELS)
    aggregated = aggregate_votes(results)

    # 5. 生成报告
    html = generate_html_report(aggregated, holdings, len(articles))
    save_report(html)

    # 6. 打印摘要
    logger.info("=== 投票结果摘要 ===")
    for rec in aggregated["final_recommendations"]:
        logger.info(f"{rec['fund_code']} -> {rec['recommendation']} (买:{rec['votes']['buy']} 卖:{rec['votes']['sell']} 持:{rec['votes']['hold']})")

if __name__ == "__main__":
    main()
