# quant/risk_control.py
import logging

logger = logging.getLogger(__name__)

def check_position_risk(holdings, cash, market_risk_level):
    """
    检查仓位风险
    返回风控建议
    """
    total_value = sum(h.get("amount", 0) for h in holdings)
    total_assets = total_value + cash
    
    if total_assets == 0:
        return {"risk_level": "low", "advice": "暂无持仓"}
    
    # 计算各维度风险
    risks = []
    
    # 1. 单一基金集中度风险（不超过20%）
    for h in holdings:
        fund_ratio = h.get("amount", 0) / total_assets * 100
        if fund_ratio > 20:
            risks.append({
                "type": "concentration",
                "fund_code": h.get("fund_code"),
                "ratio": fund_ratio,
                "advice": f"基金 {h.get('fund_code')} 仓位占比 {fund_ratio:.1f}%，建议控制在20%以内"
            })
    
    # 2. 整体仓位风险
    position_ratio = total_value / total_assets * 100
    if position_ratio > 80:
        risks.append({
            "type": "over_position",
            "ratio": position_ratio,
            "advice": f"整体仓位 {position_ratio:.1f}%，建议保留现金以应对风险"
        })
    
    # 3. 市场风险（结合估值）
    if market_risk_level and market_risk_level.get("level") == "high":
        risks.append({
            "type": "market_risk",
            "advice": market_risk_level.get("advice", "市场估值偏高，注意风险")
        })
    
    # 4. 单只基金亏损风险
    for h in holdings:
        profit_loss = h.get("profit_loss", 0)
        if profit_loss < -h.get("amount", 0) * 0.1:  # 亏损超过10%
            risks.append({
                "type": "loss",
                "fund_code": h.get("fund_code"),
                "loss_ratio": abs(profit_loss) / h.get("amount", 1) * 100,
                "advice": f"基金 {h.get('fund_code')} 亏损超过10%，建议关注"
            })
    
    risk_level = "low"
    if len(risks) >= 2:
        risk_level = "high"
    elif len(risks) >= 1:
        risk_level = "medium"
    
    return {
        "level": risk_level,
        "risks": risks,
        "total_assets": total_assets,
        "position_ratio": position_ratio
    }

def get_risk_advice_for_ai(holdings, cash, market_risk_level):
    """生成供AI使用的风控提示"""
    risk_check = check_position_risk(holdings, cash, market_risk_level)
    
    if not risk_check.get("risks"):
        return "当前持仓结构合理，无明显风险。"
    
    advice_lines = ["【风控提示】"]
    for r in risk_check["risks"]:
        advice_lines.append(f"- {r['advice']}")
    
    return "\n".join(advice_lines)
