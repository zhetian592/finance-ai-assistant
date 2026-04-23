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

# 行业/主题关键词（可扩展）
TOPIC_KEYWORDS = {
    "新能源": ["新能源", "光伏", "风电", "电动车", "锂电池", "特斯拉"],
    "消费": ["消费", "零售", "白酒", "食品", "家电"],
    "科技": ["芯片", "半导体", "AI", "人工智能", "软件", "5G"],
    "医药": ["医药", "生物", "疫苗", "CXO", "创新药"],
    "金融": ["银行", "保险", "券商", "地产"],
    "能源": ["石油", "油价", "天然气", "煤炭"],
    "债券": ["债券", "利率", "央行", "降息", "加息"],
    "宏观": ["GDP", "CPI", "PMI", "失业率", "通胀"],
}

def extract_topics(text: str) -> list:
    """从文本中提取主题"""
    text_lower = text.lower()
    matched = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                matched.append(topic)
                break
    return list(set(matched))

def get_sentiment(text: str) -> dict:
    """使用 FinBERT 判断情感（positive/negative/neutral）"""
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
    """将单条新闻转换为结构化事件"""
    title = news_item.get("title", "")
    summary = news_item.get("summary", "")
    full_text = title + ". " + summary
    topics = extract_topics(full_text)
    sentiment = get_sentiment(full_text)
    return {
        "title": title,
        "summary": summary[:200],
        "topics": topics,
        "sentiment": sentiment["label"],
        "sentiment_score": sentiment["score"],
        "url": news_item.get("link", ""),
        "timestamp": news_item.get("published", "")
    }
