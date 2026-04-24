# event_extractor.py
import re
import logging

logger = logging.getLogger(__name__)

# 主题关键词 → 申万一级行业映射
TOPIC_TO_SECTOR = {
    "新能源": ["电气设备", "汽车"],
    "消费": ["食品饮料", "家用电器", "商业贸易"],
    "科技": ["电子", "计算机", "通信"],
    "医药": ["医药生物"],
    "金融": ["银行", "非银金融"],
    "能源": ["采掘", "有色金属"],
    "债券": ["银行"],
    "宏观": [],
    "建材": ["建筑材料"],
    "地产": ["房地产", "建筑装饰"],
    "军工": ["国防军工"],
    "农业": ["农林牧渔"],
    "化工": ["化工"],
    "钢铁": ["钢铁"],
    "交运": ["交通运输"],
    "传媒": ["传媒"],
    "机械": ["机械设备"],
    "轻工": ["轻工制造"],
}

# 简单情绪词典 (用于正面/负面判断)
POSITIVE_WORDS = ["上涨", "增长", "利好", "提振", "上升", "突破", "创新高", "看好", "买入", "乐观", "强劲"]
NEGATIVE_WORDS = ["下跌", "下降", "利空", "打压", "下滑", "跌破", "亏损", "担忧", "卖出", "悲观", "疲软"]

def extract_topics_to_sectors(text: str) -> list:
    text_lower = text.lower()
    matched_sectors = set()
    for topic, sectors in TOPIC_TO_SECTOR.items():
        if topic.lower() in text_lower:
            for sec in sectors:
                matched_sectors.add(sec)
    return list(matched_sectors)

def simple_sentiment(text: str) -> dict:
    pos_count = sum(1 for w in POSITIVE_WORDS if w in text)
    neg_count = sum(1 for w in NEGATIVE_WORDS if w in text)
    if pos_count > neg_count:
        return {"label": "positive", "score": min(0.9, 0.5 + pos_count * 0.1)}
    elif neg_count > pos_count:
        return {"label": "negative", "score": min(0.9, 0.5 + neg_count * 0.1)}
    else:
        return {"label": "neutral", "score": 0.5}

def extract_event(news_item: dict) -> dict:
    title = news_item.get("title", "")
    summary = news_item.get("summary", "")
    full_text = title + ". " + summary
    sectors = extract_topics_to_sectors(full_text)
    sentiment = simple_sentiment(full_text)
    return {
        "title": title,
        "summary": summary[:200],
        "topics": sectors,
        "sentiment": sentiment["label"],
        "sentiment_score": sentiment["score"],
        "url": news_item.get("link", ""),
        "timestamp": news_item.get("published", "")
    }
