import pandas as pd
import numpy as np

def calc_valuation_score(pe_percentile):
    """PE分位越低得分越高"""
    if pe_percentile is None:
        return 50.0
    return 100 - pe_percentile

def calc_momentum_score(close_series, period=20):
    """动量得分"""
    if close_series is None or not hasattr(close_series, 'iloc') or len(close_series) < period + 1:
        return 50.0
    ret = (close_series.iloc[-1] - close_series.iloc[-period]) / close_series.iloc[-period]
    return np.clip((ret + 0.1) / 0.3 * 100, 0, 100)

def compute_sector_scores(sector_data, sentiment_map=None):
    """
    sector_data: {sector: {'val_percentile': float|None, 'close': Series|None}}
    sentiment_map: {sector: float} 情绪得分 0~100
    返回 DataFrame
    """
    if sentiment_map is None:
        sentiment_map = {}

    scores = []
    for sector, data in sector_data.items():
        val_score = calc_valuation_score(data.get('val_percentile'))
        mom_score = calc_momentum_score(data.get('close'))
        money_score = 50.0    # 资金因子暂用中性值
        sent_score = sentiment_map.get(sector, 50.0)
        total = (val_score + mom_score + money_score + sent_score) / 4
        scores.append({
            'sector': sector,
            'val_score': val_score,
            'mom_score': mom_score,
            'money_score': money_score,
            'sent_score': sent_score,
            'total_score': total
        })

    df = pd.DataFrame(scores).sort_values('total_score', ascending=False)
    return df
