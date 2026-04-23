from llm_client import llm_chat
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def analyze_sentiment(headline: str, source_lang: str = '中文') -> dict:
    prompt = f"""你是一个A股分析师。请判断以下财经新闻的情感，并提取可能受影响的申万一级行业。
返回JSON：{{"sentiment": "正面/负面/中性", "confidence": 0.0~1.0, "impact_sectors": ["行业1"]}}
新闻：{headline}"""
    messages = [{"role": "user", "content": prompt}]
    try:
        content, model = llm_chat(messages, temperature=0.1)
        # 提取第一个JSON对象
        start = content.find('{')
        end = content.rfind('}') + 1
        if start != -1 and end != -1:
            return json.loads(content[start:end])
    except Exception as e:
        logger.error(f"情感分析失败: {e}")
    # 兜底
    return {"sentiment": "中性", "confidence": 0.5, "impact_sectors": []}

def process_news_batch(headlines):
    """批量处理新闻，汇总各行业情绪得分"""
    sector_sentiment = {}
    for title, _, lang in headlines:
        res = analyze_sentiment(title, lang)
        for sector in res.get('impact_sectors', []):
            if sector not in sector_sentiment:
                sector_sentiment[sector] = []
            score = 1 if res['sentiment'] == '正面' else -1 if res['sentiment'] == '负面' else 0
            sector_sentiment[sector].append(score * res['confidence'])
    # 平均并归一化到0-100
    final_scores = {}
    for sec, scores in sector_sentiment.items():
        avg = sum(scores) / len(scores)
        final_scores[sec] = (avg + 1) * 50  # 映射到0-100
    return final_scores
