import pandas as pd
import numpy as np
from typing import List, Dict
import logging
from sector_etf_map import get_etf_for_sector

logger = logging.getLogger(__name__)

def _apply_value_trap_discount(val_score: float, mom_score: float, mom_ok: bool) -> float:
    """
    若动量低于55且估值很高（>=80），则对估值进行折价，防止落入估值陷阱
    """
    if not mom_ok or mom_score is None:
        return val_score
    if val_score >= 80 and mom_score < 55:
        discount = 0.85  # 折价15%
        logger.debug(f"估值陷阱折扣: 估值{val_score:.1f}, 动量{mom_score:.1f} -> 折价后{val_score*discount:.1f}")
        return val_score * discount
    return val_score

def allocate_weights_by_score(df: pd.DataFrame, cash: float, top_n: int = 3,
                              risk_level: str = "medium") -> List[Dict]:
    """
    根据行业得分分配资金，自动过滤无ETF的行业，应用估值陷阱折扣，
    并根据市场风险等级调整总仓位比例。
    
    参数:
        df: 包含 sector, valuation_score, money_flow_score, momentum_score, sentiment_score, total_score 等列
        cash: 总现金
        top_n: 选前N个行业
        risk_level: 风险等级 high/medium/low
    返回:
        推荐列表 [{"code": ETF代码, "name": 名称, "amount": 金额, "reason": 理由}]
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
    
    # 重新计算综合得分（加入估值陷阱折扣，并仅用有效因子）
    adjusted_scores = []
    for _, row in df_with_etf.iterrows():
        val = row.get('valuation_score', 50)
        mom = row.get('momentum_score', 50)
        mom_ok = pd.notna(row.get('momentum_score'))
        val_adjusted = _apply_value_trap_discount(val, mom, mom_ok)
        # 收集有效因子得分（已调整估值）
        scores = [val_adjusted]
        for f in ['money_flow_score', 'momentum_score', 'sentiment_score']:
            if pd.notna(row.get(f)):
                scores.append(row[f])
        total = np.mean(scores) if scores else 50.0
        adjusted_scores.append(total)
    df_with_etf['adjusted_score'] = adjusted_scores
    df_with_etf = df_with_etf.sort_values('adjusted_score', ascending=False).reset_index(drop=True)
    
    # 根据风险等级确定总仓位比例（中等风险50%，高风险40%，低风险80%）
    risk_cash_ratio = {"high": 0.4, "medium": 0.5, "low": 0.8}.get(risk_level, 0.5)
    invest_cash = cash * risk_cash_ratio
    top_n = max(1, min(top_n, len(df_with_etf)))
    top_df = df_with_etf.head(top_n).copy()
    
    # 得分归一化权重
    scores = top_df['adjusted_score'].values
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
            "reason": f"行业轮动：{row['sector']} 综合得分{row['adjusted_score']:.1f}"
        })
    return recommendations

def generate_sector_report(df: pd.DataFrame, top_n: int = 5) -> str:
    """生成行业得分表格的HTML片段，显示有效因子数量，并对低质量数据给出警告"""
    if df.empty:
        return "<p>暂无行业数据</p>"
    df_display = df.copy()
    df_display['etf_code'] = df_display['sector'].apply(lambda s: get_etf_for_sector(s)[0] or "无ETF")
    html = "<h3>📈 行业多因子得分排名</h3>"
    html += "<td><th>行业</th><th>ETF代码</th><th>估值</th><th>资金</th><th>动量</th><th>情绪</th><th>综合</th><th>有效因子</th></tr>"
    displayed = 0
    low_quality_warning = False
    for _, row in df_display.iterrows():
        if displayed >= top_n:
            break
        if row['etf_code'] == "无ETF":
            continue
        val = f"{row['valuation_score']:.1f}" if pd.notna(row['valuation_score']) else "N/A"
        money = f"{row['money_flow_score']:.1f}" if pd.notna(row['money_flow_score']) else "N/A"
        mom = f"{row['momentum_score']:.1f}" if pd.notna(row['momentum_score']) else "N/A"
        sent = f"{row['sentiment_score']:.1f}" if pd.notna(row['sentiment_score']) else "N/A"
        total = f"{row['total_score']:.1f}"
        eff = int(row['effective_count']) if pd.notna(row['effective_count']) else 0
        if eff < 2:
            low_quality_warning = True
        html += f"<tr {bg_class}><td>{row['sector']}</td><td>{row['etf_code']}</td><td>{val}</td><td>{money}</td><td>{mom}</td><td>{sent}</td><td><strong>{total}</strong></td><td>{eff}</td></tr>"
        displayed += 1
    html += "</table>"
    if low_quality_warning:
        html += "<p style='color:orange;'>⚠️ 部分行业有效因子不足2个，综合得分参考价值有限。建议检查数据源。</p>"
    return html

def generate_concentration_warning(recommendations: List[Dict]) -> str:
    """分析推荐组合的风格集中度，返回警告文本"""
    if not recommendations:
        return ""
    style_map = {
        "银行": "金融地产", "非银金融": "金融地产", "房地产": "金融地产", "建筑装饰": "金融地产",
        "建筑材料": "金融地产", "钢铁": "周期", "有色金属": "周期", "采掘": "周期",
        "化工": "周期", "电气设备": "成长", "电子": "成长", "计算机": "成长", "通信": "成长",
        "食品饮料": "消费", "家用电器": "消费", "医药生物": "消费", "农林牧渔": "消费",
        "公用事业": "防御", "交通运输": "防御", "国防军工": "军工",
    }
    styles = []
    for rec in recommendations:
        name = rec.get("name", "")
        matched = False
        for kw, style in style_map.items():
            if kw in name or kw.lower() in name.lower():
                styles.append(style)
                matched = True
                break
        if not matched:
            styles.append("其他")
    if len(styles) >= 2 and all(s == styles[0] for s in styles):
        return f"⚠️ **组合集中度风险**：当前推荐的{len(styles)}个行业全部属于【{styles[0]}】风格，单一风格暴露过高。建议搭配防御性行业（如公用事业、医药）以降低波动。"
    return ""
