import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import atexit
from data_services import DataService

logger = logging.getLogger(__name__)

# 申万一级行业 → 指数代码（用于行情查询）
SECTORS = {
    "农林牧渔": "sz.399110", "采掘": "sz.399120", "化工": "sz.399130",
    "钢铁": "sz.399140", "有色金属": "sz.399150", "电子": "sz.399160",
    "家用电器": "sz.399170", "食品饮料": "sz.399180", "纺织服装": "sz.399190",
    "医药生物": "sz.399200", "公用事业": "sz.399210", "交通运输": "sz.399220",
    "房地产": "sz.399230", "商业贸易": "sz.399240", "休闲服务": "sz.399250",
    "计算机": "sz.399260", "传媒": "sz.399270", "通信": "sz.399280",
    "国防军工": "sz.399290", "银行": "sh.000134", "非银金融": "sz.399310",
    "汽车": "sz.399320", "机械设备": "sz.399330", "建筑装饰": "sz.399340",
    "电气设备": "sz.399350", "轻工制造": "sz.399360", "建筑材料": "sz.399370",
    "综合": "sz.399380",
}

_ds = DataService()

# -------------------- 1. 估值因子 --------------------
def get_sector_valuation(sector: str) -> tuple:
    """返回 (得分, 是否有效)，得分=100-PE分位，越高越低估"""
    val = _ds.get_valuation(sector)
    if val and 'pe_percentile' in val:
        score = 100 - val['pe_percentile']
        logger.info(f"[估值] {sector}: PE分位={val['pe_percentile']:.1f}%, 得分={score:.1f}")
        return score, True
    return None, False

# -------------------- 2. 资金因子（暂不可用，返回 None）--------------------
def get_sector_money_flow(sector: str) -> tuple:
    # 北向资金接口不稳定，暂时跳过
    return None, False

# -------------------- 3. 动量因子（基于稳定行情源）--------------------
def get_sector_momentum(sector: str, days: int = 20) -> tuple:
    code = SECTORS.get(sector)
    if not code:
        return None, False
    df = _ds.get_daily_ohlcv(code, count=days+10)
    if df is None or len(df) < days+1:
        return None, False
    close = df['close'].values
    ret = (close[-1] - close[-days-1]) / close[-days-1]
    # 映射到 0~100（假设收益范围 -0.1 ~ 0.2）
    score = np.clip((ret + 0.1) / 0.3 * 100, 0, 100)
    logger.info(f"[动量] {sector}: {days}日收益={ret:.2%}, 得分={score:.1f}")
    return score, True

# -------------------- 4. 情绪因子（基于新闻事件+简单词库）--------------------
def get_sector_sentiment(sector: str, news_events: list) -> tuple:
    if not news_events:
        return None, False
    # 关键词映射：行业 → 相关关键词（可扩展）
    sector_keywords = {
        "建筑材料": ["水泥", "玻璃", "建材", "基建"],
        "银行": ["银行", "降息", "信贷", "股息"],
        "非银金融": ["券商", "保险", "证券"],
        "电子": ["芯片", "半导体", "AI", "消费电子"],
    }
    keywords = sector_keywords.get(sector, [sector])
    sentiment_scores = []
    for evt in news_events:
        text = evt.get('title', '') + " " + evt.get('summary', '')
        if any(kw in text for kw in keywords):
            sent = _ds.analyze_sentiment(text)
            if sent['label'] == 'positive':
                sentiment_scores.append(sent['score'] * 100)
            elif sent['label'] == 'negative':
                sentiment_scores.append((1 - sent['score']) * 100)
            else:
                sentiment_scores.append(50)
    if not sentiment_scores:
        return None, False
    avg_score = np.mean(sentiment_scores)
    logger.info(f"[情绪] {sector}: 相关新闻{len(sentiment_scores)}条, 得分={avg_score:.1f}")
    return avg_score, True

# -------------------- 5. 综合数据获取 --------------------
def fetch_all_sector_data(sector_list: list, news_events: list) -> pd.DataFrame:
    rows = []
    for sector in sector_list:
        val, val_ok = get_sector_valuation(sector)
        money, money_ok = get_sector_money_flow(sector)
        mom, mom_ok = get_sector_momentum(sector)
        sent, sent_ok = get_sector_sentiment(sector, news_events)
        
        valid = []
        if val_ok: valid.append(val)
        if money_ok: valid.append(money)
        if mom_ok: valid.append(mom)
        if sent_ok: valid.append(sent)
        
        if len(valid) >= 2:
            total = np.mean(valid)
        elif len(valid) == 1:
            total = valid[0]
            logger.info(f"{sector} 仅一个有效因子({valid[0]:.1f})，直接使用")
        else:
            total = 50.0
        
        rows.append({
            "sector": sector,
            "valuation_score": val if val_ok else None,
            "money_flow_score": money if money_ok else None,
            "momentum_score": mom if mom_ok else None,
            "sentiment_score": sent if sent_ok else None,
            "total_score": total,
            "effective_count": len(valid)
        })
    df = pd.DataFrame(rows)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df
