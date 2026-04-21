"""
风控模块：检查持仓集中度、整体仓位、单只亏损等
"""

def check_position_risk(holdings: dict) -> str:
    """
    根据持仓数据返回风控提示
    holdings 格式: {
        "000001": {"name": "基金A", "amount": 10000, "cost": 1.2, "current": 1.15},
        ...
    }
    """
    if not holdings:
        return "暂无持仓数据，请维护 holdings.json"

    total_value = 0.0
    max_single_ratio = 0.0
    max_single_name = ""
    loss_funds = []

    for code, info in holdings.items():
        amount = info.get("amount", 0)
        current = info.get("current", info.get("cost", 0))
        value = amount * current
        total_value += value

        # 单只基金占比
        if total_value > 0:
            ratio = value / total_value
            if ratio > max_single_ratio:
                max_single_ratio = ratio
                max_single_name = info.get("name", code)

        # 亏损检查（当前净值低于成本价）
        cost = info.get("cost", 0)
        if cost > 0 and current < cost:
            loss_rate = (cost - current) / cost
            loss_funds.append((info.get("name", code), loss_rate))

    risk_msgs = []
    if max_single_ratio > 0.3:
        risk_msgs.append(f"⚠️ 单只基金 {max_single_name} 占比 {max_single_ratio:.1%}，超过30%集中度红线")
    elif max_single_ratio > 0.2:
        risk_msgs.append(f"📌 单只基金 {max_single_name} 占比 {max_single_ratio:.1%}，接近20%建议上限")

    if len(holdings) > 5:
        risk_msgs.append(f"📌 持有基金数量 {len(holdings)} 只，超过5只，建议精简")

    for name, rate in loss_funds:
        if rate > 0.1:
            risk_msgs.append(f"🔴 {name} 亏损 {rate:.1%}，超过10%止损线")
        elif rate > 0.05:
            risk_msgs.append(f"🟡 {name} 亏损 {rate:.1%}，建议关注")

    if not risk_msgs:
        return "✅ 持仓结构健康，未触发风控阈值"
    return "\n".join(risk_msgs)


def get_risk_advice(holdings: dict, market_risk: str) -> str:
    """
    根据持仓和市场风险等级生成综合风控建议
    market_risk: 'high' / 'medium' / 'low'
    """
    risk_msg = check_position_risk(holdings)

    if market_risk == "high":
        return f"【市场风险高】{risk_msg}\n建议：降低总仓位至5成以下，增持货币或债券类资产。"
    elif market_risk == "low":
        return f"【市场风险低】{risk_msg}\n建议：可维持7成以上仓位，积极配置权益类资产。"
    else:  # medium 或未知
        return f"【市场风险中等】{risk_msg}\n建议：保持中性仓位，均衡配置。"
