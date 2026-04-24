# event_extractor.py
import re
import logging
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

logger = logging.getLogger(__name__)

# 加载 FinBERT 模型（英文）
try:
    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
    FINBERT_AVAILABLE = True
except Exception as e:
    logger.warning(f"FinBERT 加载失败: {e}，将使用规则匹配")
    FINBERT_AVAILABLE = False

# 主题关键词 → 申万一级行业映射（用于事件情绪）
TOPIC_TO_SECTOR = {
    "新能源": ["电气设备", "汽车"],
    "消费": ["食品饮料", "家用电器", "商业贸易"],
    "科技": ["电子", "计算机", "通信"],
    "医药": ["医药生物"],
    "金融": ["银行", "非银金融"],
    "能源": ["采掘", "有色金属"],
    "债券": ["银行"],
    "宏观": [],  # 宏观事件影响所有
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

def extract_topics_to_sectors(text: str) -> list:
    """从文本中提取主题，并映射到申万一级行业"""
    text_lower = text.lower()
    matched_sectors = set()
    for topic, sectors in TOPIC_TO_SECTOR.items():
        if topic.lower() in text_lower:
            for sec in sectors:
                matched_sectors.add(sec)
    return list(matched_sectors)

def get_sentiment(text: str) -> dict:
    if not FINBERT_AVAILABLE:
        return {"label": "neutral", "score": 0.5}
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    labels = ["positive", "negative", "neutral"]
    scores = probs.detach().numpy()[0]
    pred_idx = scores.argmax()
    return {"label": labels[pred_idx], "score": float(scores[pred_idx])}

def extract_event(news_item: dict) -> dict:
    """将单条新闻转换为结构化事件，包含映射后的申万行业"""
    title = news_item.get("title", "")
    summary = news_item.get("summary", "")
    full_text = title + ". " + summary
    sectors = extract_topics_to_sectors(full_text)
    sentiment = get_sentiment(full_text)
    return {
        "title": title,
        "summary": summary[:200],
        "topics": sectors,          # 申万行业名称列表
        "sentiment": sentiment["label"],
        "sentiment_score": sentiment["score"],
        "url": news_item.get("link", ""),
        "timestamp": news_item.get("published", "")
    }
