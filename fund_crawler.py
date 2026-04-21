#!/usr/bin/env python3
"""
财经AI决策辅助工具 - 主爬虫
抓取RSS新闻，调用多模型AI分析，结合量化数据生成基金报告
"""

import os
import sys
import json
import argparse
import logging
import requests
import feedparser
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 量化模块
from quant import get_market_risk_level, update_fund_nav, check_position_risk, get_risk_advice

# 配置日志
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

# AI 配置
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# 有效的基金代码前缀（用于过滤 AI 编造）
VALID_FUND_PREFIXES = ["00", "01", "02", "50", "51", "16", "18"]  # 常见基金代码开头

# ==================== 辅助函数 ====================
def load_json(file_path: str, default: Any = None) -> Any:
    """加载 JSON 文件，如果不存在或出错返回默认值"""
    if not os.path.exists(file_path):
        return default if default is not None else {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载 {file_path} 失败: {e}")
        return default if default is not None else {}

def save_json(file_path: str, data: Any):
    """保存 JSON 文件"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def is_valid_fund_code(code: str) -> bool:
    """检查基金代码是否有效（6位数字，以常见前缀开头）"""
    if not code or not isinstance(code, str):
        return False
    if not code.isdigit() or len(code) != 6:
        return False
    for prefix in VALID_FUND_PREFIXES:
        if code.startswith(prefix):
            return True
    return False

# ==================== 新闻抓取 ====================
def fetch_rss_feed(url: str, timeout: int = 15) -> List[Dict]:
    """抓取单个 RSS 源，返回文章列表"""
    try:
        feed = feedparser.parse(url)
        entries = []
        for entry in feed.entries[:10]:  # 每个源最多10条
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
    """并发抓取所有 RSS 源"""
    all_news = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(fetch_rss_feed, url): url for url in sources}
        for future in as_completed(future_to_url):
            entries = future.result()
            all_news.extend(entries)
    # 去重（按标题）
    seen = set()
    unique = []
    for item in all_news:
        title = item.get("title", "")
        if title and title not in seen:
            seen.add(title)
            unique.append(item)
    return unique

# ==================== AI 调用 ====================
def call_openrouter(prompt: str, model: str = "openrouter/free") -> Optional[str]:
    """调用 OpenRouter API"""
    if not OPENROUTER_API_KEY:
        logger.warning("未设置 OPENROUTER_API_KEY，跳过 OpenRouter")
        return None
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 1000
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            logger.error(f"OpenRouter 错误: {response.status_code} {response.text}")
            return None
    except Exception as e:
        logger.error(f"OpenRouter 调用失败: {e}")
        return None

def call_github_models(prompt: str) -> Optional[str]:
    """调用 GitHub Models (gpt-4o-mini)"""
    if not GITHUB_TOKEN:
        logger.warning("未设置 GITHUB_TOKEN，跳过 GitHub Models")
        return None
    try:
        response = requests.post(
            "https://models.inference.ai.azure.com/chat/completions",
            headers={
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 1000
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            logger.error(f"GitHub Models 错误: {response.status_code} {response.text}")
            return None
    except Exception as e:
        logger.error(f"GitHub Models 调用失败: {e}")
        return None

def get_ai_recommendations(news_text: str, holdings: List[Dict], market_risk: Dict) -> Dict:
    """调用多个 AI 模型，获取基金操作建议（投票机制）"""
    # 构建持仓信息
    holdings_info = "\n".join([
        f"- {h.get('name', h.get('code'))} (代码:{h.get('code')}) 持有{h.get('amount',0)}份 成本{h.get('cost',0)} 现价{h.get('current',0)}"
        for h in holdings
    ])
    
    prompt = f"""你是一个专业的基金投资顾问。基于以下财经新闻和市场风险分析，对持仓基金给出操作建议。

新闻摘要：
{news_text[:3000]}

市场风险等级：{market_risk.get('level', 'medium')} - {market_risk.get('advice', '')}
风险原因：{', '.join(market_risk.get('reasons', []))}

持仓基金：
{holdings_info}

请为每只基金输出 JSON 格式建议，格式如下：
[
  {{"code": "基金代码", "action": "买入/卖出/持有", "confidence": 0-100, "reason": "简短理由"}}
]
注意：action 只能是 "买入"、"卖出" 或 "持有"。不要编造不存在的基金代码。
"""
    
    # 并发调用两个模型
    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_open = executor.submit(call_openrouter, prompt)
        future_github = executor.submit(call_github_models, prompt)
        results['openrouter'] = future_open.result()
        results['github'] = future_github.result()
    
    # 解析 JSON 建议
    final_recommendations = []
    model_outputs = []
    for model_name, output in results.items():
        if output:
            model_outputs.append(output)
            try:
                # 提取 JSON 部分（有些模型会加额外文字）
                json_start = output.find('[')
                json_end = output.rfind(']') + 1
                if json_start != -1 and json_end > json_start:
                    json_str = output[json_start:json_end]
                    recs = json.loads(json_str)
                    if isinstance(recs, list):
                        final_recommendations.extend(recs)
            except Exception as e:
                logger.warning(f"解析 {model_name} 输出失败: {e}")
    
    # 投票去重：按 code 聚合，取多数 action
    vote_map = {}
    for rec in final_recommendations:
        code = rec.get("code")
        if not code or not is_valid_fund_code(code):
            continue
        action = rec.get("action")
        if action not in ["买入", "卖出", "持有"]:
            continue
        if code not in vote_map:
            vote_map[code] = {"actions": [], "reasons": []}
        vote_map[code]["actions"].append(action)
        vote_map[code]["reasons"].append(rec.get("reason", ""))
    
    # 决定最终建议
    final = []
    for code, data in vote_map.items():
        actions = data["actions"]
        # 简单投票：出现次数最多的 action
        final_action = max(set(actions), key=actions.count)
        # 置信度按模型数量归一化
        confidence = int((actions.count(final_action) / len(actions)) * 100)
        reason = "; ".join(set(data["reasons"][:2]))
        final.append({
            "code": code,
            "action": final_action,
            "confidence": confidence,
            "reason": reason
        })
    return {"recommendations": final, "raw_outputs": model_outputs}

# ==================== 报告生成 ====================
def generate_html_report(holdings: List[Dict], news: List[Dict], ai_result: Dict, 
                         market_risk: Dict, risk_advice: str, mode: str) -> str:
    """生成 HTML 报告"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    risk_level = market_risk.get("level", "unknown")
    risk_color = {"high": "red", "medium": "orange", "low": "green"}.get(risk_level, "gray")
    
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
        .buy {{ color: green; font-weight: bold; }}
        .sell {{ color: red; font-weight: bold; }}
        .hold {{ color: gray; }}
        .news-item {{ margin-bottom: 15px; padding: 10px; background: #f9f9f9; }}
    </style>
</head>
<body>
    <h1>📊 财经AI决策报告</h1>
    <p>生成时间: {timestamp}</p>
    <p>运行模式: {mode}</p>
    
    <div class="risk risk-{risk_level}">
        <h2>📈 市场风险评估</h2>
        <p>等级: <strong>{risk_level.upper()}</strong> - {market_risk.get('advice', '')}</p>
        <p>评分: {market_risk.get('score', 0)}</p>
        <ul>
"""
    for reason in market_risk.get('reasons', []):
        html += f"<li>{reason}</li>"
    
    html += f"""
        </ul>
    </div>
    
    <div class="risk risk-{risk_level}">
        <h2>🛡️ 风控建议</h2>
        <pre>{risk_advice}</pre>
    </div>
    
    <h2>💰 持仓与AI建议</h2>
    <table>
        <tr><th>基金代码</th><th>名称</th><th>持有份额</th><th>成本价</th><th>现价</th><th>AI建议</th><th>置信度</th><th>理由</th></tr>
"""
    # 构建建议映射
    rec_map = {r['code']: r for r in ai_result.get('recommendations', [])}
    for fund in holdings:
        code = fund.get('code', '')
        name = fund.get('name', '')
        amount = fund.get('amount', 0)
        cost = fund.get('cost', 0)
        current = fund.get('current', 0)
        rec = rec_map.get(code, {})
        action = rec.get('action', '持有')
        confidence = rec.get('confidence', 0)
        reason = rec.get('reason', '无AI建议')
        action_class = f"class='{action.lower()}'" if action in ['买入','卖出','持有'] else ""
        html += f"""
        <tr>
            <td>{code}</td><td>{name}</td><td>{amount}</td><td>{cost:.4f}</td><td>{current:.4f}</td>
            <td {action_class}>{action}</td><td>{confidence}%</td><td>{reason}</td>
        </tr>
"""
    html += """
    </table>
    
    <h2>📰 近期财经新闻</h2>
"""
    for idx, item in enumerate(news[:10]):
        title = item.get('title', '无标题')
        summary = item.get('summary', '')[:200]
        link = item.get('link', '#')
        html += f"""
    <div class="news-item">
        <a href="{link}" target="_blank"><strong>{title}</strong></a>
        <p>{summary}...</p>
    </div>
"""
    html += """
</body>
</html>
"""
    return html

# ==================== 主函数 ====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["recommend", "hold"], default="recommend",
                        help="recommend: 正常推荐; hold: 只读持仓不调用AI")
    args = parser.parse_args()
    
    # 1. 加载持仓
    holdings_data = load_json(HOLDINGS_FILE, {"holdings": [], "cash": 0})
    holdings_list = holdings_data.get("holdings", [])
    cash = holdings_data.get("cash", 0)
    logger.info(f"加载持仓 {len(holdings_list)} 只基金，现金 {cash} 元")
    
    # 2. 更新基金净值（使用 AKShare）
    try:
        updated_holdings = update_fund_nav(holdings_list)
        if updated_holdings:
            holdings_list = updated_holdings
            # 保存回 holdings.json（可选）
            holdings_data["holdings"] = holdings_list
            save_json(HOLDINGS_FILE, holdings_data)
            logger.info("基金净值已更新")
    except Exception as e:
        logger.warning(f"更新净值失败: {e}")
    
    # 3. 获取市场风险等级
    market_risk = get_market_risk_level()
    logger.info(f"市场风险等级: {market_risk.get('level')}")
    
    # 4. 风控建议
    risk_advice = get_risk_advice(holdings_list, cash, market_risk)
    
    # 5. 新闻抓取
    sources = load_json(SOURCES_FILE, [])
    if not sources:
        # 默认 RSS 源
        sources = [
            "https://feeds.bloomberg.com/markets/news.rss",
            "https://feeds.bloomberg.com/economics/news.rss",
            "http://feeds.bbci.co.uk/news/business/rss.xml",
            "https://www.wsj.com/xml/rss/3_7085.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        ]
    logger.info(f"开始抓取 {len(sources)} 个 RSS 源")
    news = fetch_all_news(sources)
    logger.info(f"抓取到 {len(news)} 条新闻")
    
    # 6. AI 分析（仅在 recommend 模式下）
    ai_result = {"recommendations": [], "raw_outputs": []}
    if args.mode == "recommend" and (OPENROUTER_API_KEY or GITHUB_TOKEN):
        news_text = "\n".join([f"{n['title']}: {n['summary'][:300]}" for n in news[:15]])
        logger.info("调用 AI 模型进行分析...")
        ai_result = get_ai_recommendations(news_text, holdings_list, market_risk)
        logger.info(f"AI 分析完成，获得 {len(ai_result['recommendations'])} 条建议")
    else:
        logger.info("跳过 AI 分析（hold 模式或缺少 API Key）")
    
    # 7. 生成 HTML 报告
    html_content = generate_html_report(holdings_list, news, ai_result, market_risk, risk_advice, args.mode)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logger.info(f"报告已保存至 {REPORT_FILE}")
    
    # 8. 保存回测记录（推荐模式下记录 AI 建议）
    if args.mode == "recommend" and ai_result['recommendations']:
        os.makedirs(os.path.dirname(RECOMMENDATIONS_FILE), exist_ok=True)
        # 加载已有记录
        existing = load_json(RECOMMENDATIONS_FILE, [])
        new_record = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "recommendations": ai_result['recommendations'],
            "market_risk": market_risk.get("level")
        }
        existing.append(new_record)
        save_json(RECOMMENDATIONS_FILE, existing)
        logger.info(f"回测记录已追加至 {RECOMMENDATIONS_FILE}")
    else:
        # 确保文件存在（即使为空）
        if not os.path.exists(RECOMMENDATIONS_FILE):
            save_json(RECOMMENDATIONS_FILE, [])
            logger.info("创建空的回测记录文件")
    
    logger.info("任务完成")

if __name__ == "__main__":
    main()
