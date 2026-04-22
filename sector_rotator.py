import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

def allocate_weights_by_score(df: pd.DataFrame, cash: float, top_n: int = 3, 
                              risk_level: str = "medium") -> List[Dict]:
    """
    根据行业得分分配资金
    df: 包含 sector, etf_code, total_score 列
    cash: 总资金
    top_n: 选择前N个行业
    risk_level: 风险等级 high/medium/low，影响仓位比例
    返回: [{"code": etf_code, "name": sector, "amount": int, "reason": str}]
    """
    if df.empty:
        return []
    # 根据风险等级调整选行业数
    if risk_level == "high":
        top_n = max(1, top_n - 1)  # 高风险减少行业分散
        total_cash_ratio = 0.5     # 只动用50%资金
    elif risk_level == "low":
        top_n = min(5, top_n + 1)  # 低风险可多配几个行业
        total_cash_ratio = 0.9
    else:  # medium
        total_cash_ratio = 0.7
    invest_cash = cash * total_cash_ratio
    # 取前top_n行业
    top_df = df.head(top_n).copy()
    # 归一化得分作为权重
    scores = top_df['total_score'].values
    weights = scores / scores.sum()
    recommendations = []
    for idx, row in top_df.iterrows():
        amount = int(invest_cash * weights[idx])
        if amount <= 0:
            continue
        recommendations.append({
            "code": row['etf_code'],
            "name": row['sector'],
            "amount": amount,
            "reason": f"行业轮动：{row['sector']} 综合得分{row['total_score']:.1f}（估值{row['valuation_score']:.0f}，资金{row['money_flow_score']:.0f}，动量{row['momentum_score']:.0f}，情绪{row['sentiment_score']:.0f}）"
        })
    # 剩余现金不投资，留作现金
    return recommendations

def generate_sector_report(df: pd.DataFrame, top_n: int = 5) -> str:
    """生成行业得分表格的HTML片段"""
    html = "<h3>📈 行业多因子得分排名</h3>"
    html += "<table><th>行业</th><th>ETF代码</th><th>估值得分</th><th>资金得分</th><th>动量得分</th><th>情绪得分</th><th>综合得分</th></tr>"
    for _, row in df.head(top_n).iterrows():
        html += f"<tr><td>{row['sector']}</td><td>{row['etf_code']}</td><td>{row['valuation_score']:.1f}</td><td>{row['money_flow_score']:.1f}</td><td>{row['momentum_score']:.1f}</td><td>{row['sentiment_score']:.1f}</td><td><strong>{row['total_score']:.1f}</strong></td></tr>"
    html += "</table>"
    return html
