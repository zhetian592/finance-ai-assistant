import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import atexit
from data_services import DataService

logger = logging.getLogger(__name__)

# 申万一级行业 → baostock/其他数据源代码（保留原映射）
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

# 全局数据服务实例
_ds = DataService()

# ==================== 估值因子 ====================
def get_sector_valuation(sector_name: str) -> tuple:
    """
    从本地估值文件获取 PE 历史分位，返回 (得分, 是否有效)
    得分 = 100 - pe_percentile，分位越低得分越高
    """
    val = _ds.get_valuation(sector_name)
    if val and 'pe_percentile' in val:
        pe_pct = val['pe_percentile']
        score = 100 - pe_pct
        logger.info(f"[估值] {sector_name}: PE分位={pe_pct:.1f}%, 得分={score:.1f}")
        return score, True
    return None, False

# ==================== 资金因子（暂时不可用） ====================
def get_sector_money_flow(sector_name: str):
    logger.debug(f"北向资金接口暂时不可用，跳过 {sector_name}")
    return None, False

# ==================== 动量因子（基于 DataService 的行情数据） ====================
def get_sector_momentum(sector_name: str, days: int = 20) -> tuple:
    code = SECTORS.get(sector_name)
    if not code:
        return None, False
    
    # 通过 DataService 获取收盘价序列，内部已实现降级
    df = _ds.get_daily_ohlcv(code, count=days+10)
    if df is None or len(df) < days+1:
        return None, False
    
    close_series = df['close']
    ret = (close_series.iloc[-1] - close_series.iloc[-days-1]) / close_series.iloc[-days-1]
    # 将收益率映射到 0~100 分（假设收益范围 -0.1 ~ 0.2）
    score = np.clip((ret + 0.1) / 0.3 * 100, 0, 100)
    logger.info(f"[动量] {sector_name}: {days}日收益={ret:.2%}, 得分={score:.1f}")
    return score, True

# ==================== 情绪因子（暂不可用） ====================
def get_sector_sentiment(sector_name: str, news_events: list):
    if not news_events:
        return None, False
    # 原有的简单关键词匹配逻辑（可保留，但通常无匹配）
    # 省略具体实现，直接返回 None
    return None, False

# ==================== 综合数据获取（与原逻辑相同） ====================
def fetch_all_sector_data(sector_list: list, news_events: list) -> pd.DataFrame:
    rows = []
    for sector in sector_list:
        val, val_ok = get_sector_valuation(sector)
        money, money_ok = get_sector_money_flow(sector)
        mom, mom_ok = get_sector_momentum(sector)
        sent, sent_ok = get_sector_sentiment(sector, news_events)
        
        valid_scores = []
        if val_ok:
            valid_scores.append(val)
        if money_ok:
            valid_scores.append(money)
        if mom_ok:
            valid_scores.append(mom)
        if sent_ok:
            valid_scores.append(sent)
        
        if len(valid_scores) >= 2:
            total_score = np.mean(valid_scores)
        elif len(valid_scores) == 1:
            total_score = valid_scores[0]
            logger.info(f"{sector} 仅有一个有效因子({valid_scores[0]:.1f})，直接使用")
        else:
            total_score = 50.0
        
        rows.append({
            "sector": sector,
            "valuation_score": val if val_ok else None,
            "money_flow_score": money if money_ok else None,
            "momentum_score": mom if mom_ok else None,
            "sentiment_score": sent if sent_ok else None,
            "total_score": total_score,
            "effective_count": len(valid_scores),
        })
    df = pd.DataFrame(rows)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df
