import pandas as pd
import numpy as np
from typing import List, Dict
import logging
from sector_etf_map import get_etf_for_sector

logger = logging.getLogger(__name__)

def allocate_weights_by_score(df: pd.DataFrame, cash: float, top_n: int = 3,
                              risk_level: str = "medium") -> List[Dict]:
    """
    根据行业得分分配资金，自动过滤无ETF的行业
    """
    if df.empty:
        return []
    
    # 过滤出有ETF的行业
    df_with_etf = df.copy()
    df_with_etf['etf_code'] = df_with_etf['sector'].apply(lambda s: get_etf_for_sector(s)[0])
    df_with_etf = df_with_etf[df_with_etf['etf_code'].notna()].copy()
    
    if df_with_etf.empty:
        logger.warning("没有可用的行业ETF，无法生成推荐")
        return []
    
    # 根据风险等级调整仓位比例和行业数量
    if risk_level == "high":
        top_n = max(1, top_n - 1)
        total_cash_ratio = 0.5
    elif risk_level == "low":
        top_n = min(5, top_n + 1)
        total_cash_ratio = 0.9
    else:
        total_cash_ratio = 0.7
    
    invest_cash = cash * total_cash_ratio
    top_df = df_with_etf.head(top_n).copy()
    
    scores = top_df['total_score'].values
    if scores.sum() == 0:
        weights = np.ones(len(scores)) / len(scores)
    else:
        weights = scores / scores.sum()
    
    recommendations = []
    for i, (_, row) in enumerate(top_df.iterrows()):
        amount = int(invest_cash * weights[i])
        if amount <= 0:
            continue
        etf_code = row['etf_code']
        etf_name = get_etf_for_sector(row['sector'])[1]
        recommendations.append({
            "code": etf_code,
            "name": f"{row['sector']}ETF({etf_name})",
            "amount": amount,
            "reason": f"行业轮动：{row['sector']} 综合得分{row['total_score']:.1f}（估值{row['valuation_score']:.0f}，资金{row['money_flow_score']:.0f}，动量{row['momentum_score']:.0f}，情绪{row['sentiment_score']:.0f}）"
        })
    return recommendations

def generate_sector_report(df: pd.DataFrame, top_n: int = 5) -> str:
    """生成行业得分表格的HTML片段，只显示有ETF的行业"""
    if df.empty:
        return "<p>暂无行业数据</p>"
    
    # 添加ETF代码列
    df_display = df.copy()
    df_display['etf_code'] = df_display['sector'].apply(lambda s: get_etf_for_sector(s)[0] or "无ETF")
    
    html = "<h3>📈 行业多因子得分排名（仅显示有ETF的行业）</h3>"
    html += "<table><th>行业</th><th>ETF代码</th><th>估值得分</th><th>资金得分</th><th>动量得分</th><th>情绪得分</th><th>综合得分</th></tr>"
    
    displayed = 0
    for _, row in df_display.iterrows():
        if displayed >= top_n:
            break
        if row['etf_code'] == "无ETF":
            continue
        score = row['total_score']
        bg_class = "style='background-color:#c8e6c9'" if score >= 70 else ("style='background-color:#fff9c4'" if score >= 40 else "style='background-color:#ffcdd2'")
        html += f"<tr {bg_class}><td>{row['sector']}</td><td>{row['etf_code']}</td><td>{row['valuation_score']:.1f}</td><td>{row['money_flow_score']:.1f}</td><td>{row['momentum_score']:.1f}</td><td>{row['sentiment_score']:.1f}</td><td><strong>{row['total_score']:.1f}</strong></td></tr>"
        displayed += 1
    html += "</table>"
    if displayed == 0:
        html = "<p>⚠️ 当前所有行业均无可交易ETF，请检查映射表。</p>"
    return html
