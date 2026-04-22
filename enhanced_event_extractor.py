import re
import jieba
import jieba.posseg as pseg
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import logging

logger = logging.getLogger(__name__)

# 加载FinBERT
try:
    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
    FINBERT_AVAILABLE = True
except:
    FINBERT_AVAILABLE = False
    logger.warning("FinBERT not available, fallback to rule-based sentiment")

# 自定义行业词典（可扩展）
INDUSTRY_DICT = {
    "新能源": ["新能源", "光伏", "风电", "电动车", "锂电池", "储能", "氢能", "特斯拉", "宁德时代"],
    "半导体": ["半导体", "芯片", "光刻机", "集成电路", "封测", "晶圆", "台积电"],
    "消费": ["消费", "白酒", "食品饮料", "家电", "免税", "旅游", "餐饮"],
    "医药": ["医药", "生物医药", "CXO", "创新药", "疫苗", "医疗器械", "医疗服务"],
    "金融": ["银行", "保险", "券商", "信托", "金融科技"],
    "周期": ["煤炭", "钢铁", "有色", "化工", "原油", "天然气", "大宗商品"],
    "科技": ["AI", "人工智能", "大数据", "云计算", "软件", "5G", "通信"],
    "军工": ["军工", "航天", "国防", "船舶"],
    "地产": ["地产", "房地产", "物业", "基建"],
    "农业": ["农业", "养猪", "种业", "化肥", "农药"]
}

# 实体类型
ENTITY_TYPES = {
    "company": r'([\u4e00-\u9fa5]{2,}(?:集团|股份|有限|公司|银行|保险|证券|基金))',
    "product": r'([\u4e00-\u9fa5]{2,}(?:车|手机|电脑|芯片|电池|药))',
    "index": r'(上证|深证|创业板|科创|沪深300|中证500|恒生|标普|纳斯达克)'
}

def load_company_dict():
    """加载常见上市公司名称词典（可手动维护）"""
    companies = [
        "贵州茅台", "宁德时代", "比亚迪", "腾讯控股", "阿里巴巴", "美团", "药明康德",
        "迈瑞医疗", "恒瑞医药", "中国平安", "招商银行", "中信证券", "隆基绿能",
        "通威股份", "阳光电源", "中芯国际", "韦尔股份", "兆易创新"
    ]
    for comp in companies:
        jieba.add_word(comp)

load_company_dict()

def extract_entities(text):
    """提取实体：公司、产品、指数"""
    entities = {"companies": [], "products": [], "indices": []}
    # 公司名
    companies = re.findall(ENTITY_TYPES["company"], text)
    entities["companies"] = list(set(companies))
    # 产品
    products = re.findall(ENTITY_TYPES["product"], text)
    entities["products"] = list(set(products))
    # 指数
    indices = re.findall(ENTITY_TYPES["index"], text)
    entities["indices"] = list(set(indices))
    return entities

def extract_industry(text):
    """识别行业主题"""
    text_lower = text.lower()
    industries = []
    for ind, keywords in INDUSTRY_DICT.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                industries.append(ind)
                break
    return list(set(industries))

def get_sentiment(text):
    """FinBERT情感分析"""
    if not FINBERT_AVAILABLE:
        # 简单规则：正面词 vs 负面词
        pos_words = ["上涨", "增长", "利好", "突破", "新高", "放量", "买入", "推荐"]
        neg_words = ["下跌", "下滑", "利空", "风险", "警告", "亏损", "卖出", "回避"]
        pos_cnt = sum(1 for w in pos_words if w in text)
        neg_cnt = sum(1 for w in neg_words if w in text)
        if pos_cnt > neg_cnt:
            return {"label": "positive", "score": 0.7}
        elif neg_cnt > pos_cnt:
            return {"label": "negative", "score": 0.7}
        else:
            return {"label": "neutral", "score": 0.5}
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    labels = ["positive", "negative", "neutral"]
    scores = probs.detach().numpy()[0]
    pred_idx = scores.argmax()
    return {"label": labels[pred_idx], "score": float(scores[pred_idx])}

def extract_event(news_item):
    """主函数：从新闻中提取结构化事件"""
    title = news_item.get("title", "")
    summary = news_item.get("summary", "")
    full_text = title + "。 " + summary
    industries = extract_industry(full_text)
    entities = extract_entities(full_text)
    sentiment = get_sentiment(full_text)
    return {
        "title": title,
        "summary": summary[:300],
        "industries": industries,
        "entities": entities,
        "sentiment": sentiment["label"],
        "sentiment_score": sentiment["score"],
        "timestamp": news_item.get("published", ""),
        "url": news_item.get("link", "")
    }
