#!/usr/bin/env python3
"""
财经AI决策辅助工具 - 主爬虫
- 有持仓时：对持仓基金给出买入/卖出/持有建议
- 无持仓时：根据新闻和市场风险推荐买入哪些基金（含真实代码、建议金额、理由）
- 自动过滤AI编造的基金代码
"""

import os
import sys
import json
import re
import argparse
import logging
import requests
import feedparser
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# 量化模块
from quant import get_market_risk_level, update_fund_nav, get_risk_advice

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

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# 有效基金代码前缀（用于初步过滤）
VALID_FUND_PREFIXES = ["00", "01", "02", "50", "51", "16", "18"]

# 内置热门基金白名单（代码 -> 名称），用于快速验证和纠正AI输出
# 可以从常见基金列表维护，此处示例包含部分热门基金
FUND_WHITELIST = {
    "000011": "华夏大盘精选混合",
    "000051": "华夏沪深300ETF联接",
    "040040": "华安纯债债券A",
    "110022": "易方达消费行业",
    "160415": "大成沪深300指数增强",
    "163402": "兴全趋势投资混合",
    "519069": "汇添富价值精选混合",
    "161725": "招商中证白酒指数",
    "003095": "中欧医疗健康混合A",
    "001594": "天弘中证银行ETF联接",
    "002001": "华夏回报混合A",
    "050001": "博时价值增长混合",
    "070003": "嘉实稳健混合",
    "090003": "大成蓝筹稳健混合",
    "100026": "富国天合稳健优选",
    "110003": "易方达上证50指数A",
    "160505": "博时主题行业混合",
    "180012": "银华富裕主题混合",
    "200008": "长城品牌优选混合",
    "213008": "宝盈资源优选混合",
    "240004": "华宝动力组合混合",
    "260108": "景顺长城新兴成长混合",
    "270005": "广发聚丰混合A",
    "288002": "华夏收入混合",
    "320003": "诺安先锋混合",
    "340007": "兴全社会责任混合",
    "360005": "光大保德信红利混合",
    "519001": "银华价值优选混合",
    "530003": "建信优选成长混合A",
    "540003": "汇丰晋信动态策略混合A",
    "550002": "信诚精萃成长混合",
    "560003": "益民创新优势混合",
    "570001": "诺德价值优势混合",
    "580002": "东吴双动力混合A",
    "590001": "中邮核心优选混合",
    "610001": "信达澳银领先增长混合",
    "620001": "金元顺安宝石动力混合",
    "630001": "华商领先企业混合",
    "660001": "农银行业成长混合",
    "690001": "民生加银品牌蓝筹混合",
}

CASH_AMOUNT = 50000  # 默认现金

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

def is_valid_fund_code(code: str) -> bool:
    """快速校验基金代码格式"""
    if not code or not isinstance(code, str):
        return False
    if not code.isdigit() or len(code) != 6:
        return False
    return any(code.startswith(p) for p in VALID_FUND_PREFIXES)

def verify_fund_code(code: str) -> tuple:
    """
    验证基金代码真实性，返回 (是否有效, 标准名称)
    优先使用白名单，也可扩展调用AKShare验证
    """
    if not is_valid_fund_code(code):
        return False, None
    # 白名单校验
    if code in FUND_WHITELIST:
        return True, FUND_WHITELIST[code]
    # 可选：调用AKShare实时验证（较慢，可能失败）
    try:
        import akshare as ak
        df = ak.fund_individual_basic_info_em(fund=code)
        if not df.empty:
            name = df.iloc[0].get('基金简称', '')
            if name:
                return True, name
    except Exception:
        pass
    # 未在白名单且验证失败，认为不可靠
    return False, None

def clean_text(text: str, max_len=30) -> str:
    if not isinstance(text, str):
        return "AI分析后建议"
    cleaned = re.sub(r'https?://\S+', '', text)
    cleaned = re.sub(r'www\.\S+', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    if not cleaned or not re.search(r'[\u4e00-\u9fff]', cleaned):
        return "AI分析后建议"
    return cleaned[:max_len]

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
    if not OPENROUTER_API_KEY:
        logger.warning("未设置 OPENROUTER_API_KEY")
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
                "temperature": 0.2,
                "max_tokens": 1000
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            logger.error(f"OpenRouter 错误: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"OpenRouter 调用失败: {e}")
        return None

def call_github_models(prompt: str) -> Optional[str]:
    if not GITHUB_TOKEN:
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
                "temperature": 0.2,
                "max_tokens": 1000
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            logger.error(f"GitHub Models 错误: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"GitHub Models 调用失败: {e}")
        return None

def get_ai_recommendations(news_text: str, holdings: List[Dict], market_risk: Dict, cash: float) -> Dict:
    """
    根据持仓是否为空，生成不同的 AI 输出：
    - 有持仓：对每只基金给出买入/卖出/持有建议
    - 无持仓：推荐买入的基金列表（代码、名称、建议买入金额、理由）
    输出经过真实代码验证过滤
    """
    is_empty = len(holdings) == 0

    if is_empty:
        # 推荐买入模式 - 强制要求输出金额（元）和真实代码
        prompt = f"""你是一个专业的基金投资顾问。用户有现金 {cash} 元，当前没有持仓。
请根据以下新闻和市场风险，推荐 **最多3只** 公募基金买入。
严格按 JSON 数组输出，格式：
[
  {{"code": "6位真实基金代码", "name": "基金名称", "amount": 建议买入金额（整数，单位：元）, "reason": "中文理由，不超过30字"}}
]
要求：
- 基金代码必须真实存在（常见的股票型、混合型、债券型基金，以00/01/02/50/51/16/18开头）
- 建议买入金额总和不超过 {cash} 元，每只金额建议为整数（如 15000）
- 理由必须中文，不含网址
- 不要编造代码，如果不知道真实代码，请使用以下示例中的有效代码：110022, 040040, 160415, 000011, 519069

新闻摘要：
{news_text[:3000]}

市场风险：{market_risk.get('level', 'medium')} - {market_risk.get('advice', '')}
原因：{', '.join(market_risk.get('reasons', []))}

只输出 JSON 数组："""
    else:
        # 持仓操作建议模式（保持不变）
        holdings_info = "\n".join([
            f"- {h.get('name', h.get('code'))} (代码:{h.get('code')}) 持有{h.get('amount',0)}份 成本{h.get('cost',0)} 现价{h.get('current',0)}"
            for h in holdings
        ])
        prompt = f"""你是一个专业的基金投资顾问。基于以下新闻和市场风险，对持仓基金给出操作建议。
严格按 JSON 数组输出，格式：
[
  {{"code": "基金代码", "action": "买入/卖出/持有", "confidence": 0-100, "reason": "中文理由，不超过30字"}}
]
action 只能是"买入"/"卖出"/"持有"。
新闻：
{news_text[:3000]}

市场风险：{market_risk.get('level', 'medium')} - {market_risk.get('advice', '')}
原因：{', '.join(market_risk.get('reasons', []))}

持仓：
{holdings_info}

只输出 JSON 数组："""

    # 并发调用两个模型
    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_open = executor.submit(call_openrouter, prompt)
        future_github = executor.submit(call_github_models, prompt)
        results['openrouter'] = future_open.result()
        results['github'] = future_github.result()

    # 解析所有输出
    all_items = []
    for model_name, output in results.items():
        if not output:
            continue
        try:
            start = output.find('[')
            end = output.rfind(']') + 1
            if start == -1 or end == 0:
                logger.warning(f"{model_name} 输出无 JSON 数组: {output[:200]}")
                continue
            json_str = output[start:end]
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    if is_empty:
                        # 推荐买入模式：验证代码真实性
                        code = item.get("code", "").strip()
                        valid, real_name = verify_fund_code(code)
                        if not valid:
                            logger.warning(f"过滤无效基金代码: {code}")
                            continue
                        # 金额处理
                        amount = item.get("amount", 0)
                        if not isinstance(amount, (int, float)) or amount <= 0:
                            amount = int(cash / 3)  # 默认分配
                        amount = int(amount)
                        # 名称使用真实名称
                        name = real_name if real_name else item.get("name", code)
                        reason = clean_text(item.get("reason", ""))
                        all_items.append({
                            "code": code,
                            "name": name,
                            "amount": amount,
                            "reason": reason
                        })
                    else:
                        # 操作建议模式：验证代码
                        code = item.get("code")
                        if not is_valid_fund_code(code):
                            continue
                        action = item.get("action")
                        if action not in ["买入", "卖出", "持有"]:
                            continue
                        reason = clean_text(item.get("reason", ""))
                        confidence = min(100, max(0, item.get("confidence", 50)))
                        all_items.append({
                            "code": code,
                            "action": action,
                            "confidence": confidence,
                            "reason": reason
                        })
        except Exception as e:
            logger.warning(f"解析 {model_name} 输出失败: {e}")

    # 投票/去重
    if is_empty:
        # 推荐买入：按 code 去重，并确保总金额不超过现金
        seen_codes = set()
        unique_items = []
        total_amount = 0
        for item in all_items:
            code = item["code"]
            if code not in seen_codes:
                seen_codes.add(code)
                # 限制金额不超过剩余现金
                if total_amount + item["amount"] > cash:
                    item["amount"] = cash - total_amount
                if item["amount"] <= 0:
                    continue
                total_amount += item["amount"]
                unique_items.append(item)
        # 如果总额小于现金，可调整最后一笔
        if unique_items and total_amount < cash:
            unique_items[-1]["amount"] += (cash - total_amount)
        return {"recommendations": unique_items, "raw_outputs": [o for o in results.values() if o]}
    else:
        # 操作建议：投票
        vote_map = {}
        for item in all_items:
            code = item["code"]
            if code not in vote_map:
                vote_map[code] = {"actions": [], "reasons": []}
            vote_map[code]["actions"].append(item["action"])
            vote_map[code]["reasons"].append(item["reason"])
        final = []
        for code, data in vote_map.items():
            actions = data["actions"]
            final_action = max(set(actions), key=actions.count)
            confidence = int((actions.count(final_action) / len(actions)) * 100)
            reason_counts = {}
            for r in data["reasons"]:
                if r:
                    reason_counts[r] = reason_counts.get(r, 0) + 1
            best_reason = max(reason_counts, key=reason_counts.get) if reason_counts else "AI分析后建议"
            final.append({
                "code": code,
                "action": final_action,
                "confidence": confidence,
                "reason": best_reason
            })
        return {"recommendations": final, "raw_outputs": [o for o in results.values() if o]}

# ==================== 报告生成 ====================
def generate_html_report(holdings: List[Dict], news: List[Dict], ai_result: Dict,
                         market_risk: Dict, risk_advice: str, mode: str, cash: float) -> str:
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
        rec_list = ai_result.get('recommendations', [])
        html += "<h2>🌟 AI 推荐买入（基于当前新闻和市场风险）</h2>"
        if rec_list:
            html += """
            <table>
                <tr><th>基金代码</th><th>基金名称</th><th>建议买入金额（元）</th><th>理由</th></tr>
            """
            for rec in rec_list:
                code = rec.get('code', '')
                name = rec.get('name', '')
                amount = rec.get('amount', 0)
                reason = rec.get('reason', '')
                html += f"<tr><td>{code}</td><td>{name}</td><td>{amount}</td><td>{reason}</td></tr>"
            html += "</table>"
            html += f"<p>💡 建议使用现金 {cash} 元，按上述金额买入。可根据风险偏好调整。</p>"
        else:
            html += "<p>⚠️ 未能获取到有效的AI推荐（所有推荐代码均无效或未通过验证），请稍后重试或手动选择基金。</p>"
    else:
        # 持仓操作建议表格（略，保持原样）
        html += "<h2>💰 持仓与AI建议</h2><table><th>基金代码</th><th>名称</th><th>持有份额</th><th>成本价</th><th>现价</th><th>AI建议</th><th>置信度</th><th>理由</th></tr>"
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
            action_class = f"class='{action.lower()}'"
            html += f"<tr><td>{code}</td><td>{name}</td><td>{amount}</td><td>{cost:.4f}</td><td>{current:.4f}</td><td {action_class}>{action}</td><td>{confidence}%</td><td>{reason}</td></tr>"
        html += "</table>"

    html += "<h2>📰 近期财经新闻</h2>"
    for item in news[:10]:
        title = item.get('title', '无标题')
        summary = item.get('summary', '')[:200]
        link = item.get('link', '#')
        html += f'<div class="news-item"><a href="{link}" target="_blank"><strong>{title}</strong></a><p>{summary}...</p></div>'
    html += "</body></html>"
    return html

# ==================== 主函数 ====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["recommend", "hold"], default="recommend")
    args = parser.parse_args()

    raw_data = load_json(HOLDINGS_FILE, {"holdings": [], "cash": CASH_AMOUNT})
    if isinstance(raw_data, list):
        holdings_list = raw_data
        cash = CASH_AMOUNT
    else:
        holdings_list = raw_data.get("holdings", [])
        cash = raw_data.get("cash", CASH_AMOUNT)
    logger.info(f"加载持仓 {len(holdings_list)} 只基金，现金 {cash} 元")

    # 更新净值（如果有持仓）
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
    logger.info(f"市场风险等级: {market_risk.get('level')}")
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
    logger.info(f"抓取 {len(sources)} 个 RSS 源")
    news = fetch_all_news(sources)
    logger.info(f"抓取到 {len(news)} 条新闻")

    ai_result = {"recommendations": [], "raw_outputs": []}
    if args.mode == "recommend" and (OPENROUTER_API_KEY or GITHUB_TOKEN):
        news_text = "\n".join([f"{n['title']}: {n['summary'][:300]}" for n in news[:15]])
        logger.info("调用 AI 模型进行分析...")
        ai_result = get_ai_recommendations(news_text, holdings_list, market_risk, cash)
        logger.info(f"AI 分析完成，获得 {len(ai_result['recommendations'])} 条建议")
    else:
        logger.info("跳过 AI 分析")

    html = generate_html_report(holdings_list, news, ai_result, market_risk, risk_advice, args.mode, cash)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"报告已保存至 {REPORT_FILE}")

    if args.mode == "recommend" and ai_result['recommendations']:
        os.makedirs(os.path.dirname(RECOMMENDATIONS_FILE), exist_ok=True)
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
        if not os.path.exists(RECOMMENDATIONS_FILE):
            save_json(RECOMMENDATIONS_FILE, [])
            logger.info("创建空回测记录")

    logger.info("任务完成")

if __name__ == "__main__":
    main()
