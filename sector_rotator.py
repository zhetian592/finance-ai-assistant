import pandas as pd
import numpy as np

def calc_valuation_score(pe_percentile):
    """pe_percentile: 当前PE在历史中的分位（0=最低,100=最高）"""
    # 分位越低，估值越便宜，得分越高
    return 100 - pe_percentile

def calc_momentum_score(close_series, period=20):
    if len(close_series) < period+1:
        return 50.0
    ret = (close_series.iloc[-1] - close_series.iloc[-period]) / close_series.iloc[-period]
    return np.clip((ret + 0.1) / 0.3 * 100, 0, 100)

def calc_money_flow_score(net_flow):
    """net_flow: 净流入金额(亿)，映射到0-100"""
    return np.clip(50 + net_flow * 5, 0, 100)  # 假设正负几亿

def calc_crowding_penalty(close_series, benchmark_returns):
    """计算拥挤度惩罚系数（0-1），1表示无拥挤"""
    if len(close_series) < 60:
        return 1.0
    # 用过去60天行业内个股收益离散度？简化：用波动率比率
    rolling_ret = close_series.pct_change().dropna()
    vol = rolling_ret.tail(20).std()
    bench_vol = benchmark_returns.tail(20).std() if benchmark_returns is not None else 0.02
    relative_vol = vol / bench_vol if bench_vol > 0 else 1
    # 相对波动率超过1.5，惩罚系数线性下降
    penalty = min(1.0, 1.5 / relative_vol)
    return penalty

def compute_sector_scores(data_dict, benchmark_close=None):
    """
    data_dict: {sector: {'val_percentile': float, 'money_flow': float, 'close': Series}}
    返回 DataFrame，包含各因子得分、拥挤惩罚、总得分。
    """
    scores = []
    for sector, data in data_dict.items():
        val_score = calc_valuation_score(data['val_percentile']) if data['val_percentile'] else 50
        mom_score = calc_momentum_score(data['close']) if data['close'] is not None else 50
        money_score = calc_money_flow_score(data['money_flow'])
        # 情绪得分从外部传入（此处暂留，实际由event_extractor更新）
        sent_score = 50
        # 拥挤惩罚
        bench_ret = benchmark_close.pct_change().dropna() if benchmark_close is not None else None
        crowding_penalty = calc_crowding_penalty(data['close'], bench_ret)
        # 综合得分 = (四因子等权) * 拥挤惩罚
        raw = (val_score + mom_score + money_score + sent_score) / 4
        final = raw * crowding_penalty
        scores.append({
            'sector': sector,
            'val_score': val_score,
            'mom_score': mom_score,
            'money_score': money_score,
            'sent_score': sent_score,
            'crowding_penalty': crowding_penalty,
            'total_score': final
        })
    df = pd.DataFrame(scores).sort_values('total_score', ascending=False)
    return df
