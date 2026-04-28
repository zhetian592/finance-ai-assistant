import pandas as pd
import numpy as np
import logging
from data_services import DataService

logger = logging.getLogger(__name__)

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

def get_sector_valuation(sector: str):
    val = _ds.get_valuation(sector)
    if val and 'pe_percentile' in val:
        score = 100 - val['pe_percentile']
        logger.info(f"[估值] {sector}: PE分位={val['pe_percentile']:.1f}%, 得分={score:.1f}")
        return score, True
    return None, False

def get_sector_money_flow(sector: str):
    return None, False

def get_sector_momentum(sector: str, days: int = 20):
    code = SECTORS.get(sector)
    if not code:
        return None, False
    df = _ds.get_daily_ohlcv(code, count=days+10)
    if df is None or len(df) < days+1:
        return None, False
    close = df['close'].values
    ret = (close[-1] - close[-days-1]) / close[-days-1]
    score = np.clip((ret + 0.1) / 0.3 * 100, 0, 100)
    logger.info(f"[动量] {sector}: {days}日收益={ret:.2%}, 得分={score:.1f}")
    return score, True

def get_sector_sentiment(sector: str, news_events: list):
    # 情绪因子已简化，直接用中性值
    return None, False

def fetch_all_sector_data(sector_list: list, news_events: list):
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
