"""
风控模块：检查持仓集中度、整体仓位、单只亏损等
"""

def check_position_risk(holdings: dict) -> str:
    """
    根据持仓字典返回风控提示
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

        if total_value > 0:
            ratio = value / total_value
            if ratio > max_single_ratio:
                max_single_ratio = ratio
                max_single_name = info.get("name", code)

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


def get_risk_advice(holdings_list: list, cash: float, market_risk: dict) -> str:
    """
    根据持仓列表、现金和市场风险等级生成综合风控建议
    holdings_list: 列表格式，每个元素是字典，例如：
        [
            {"code": "000001", "name": "基金A", "amount": 10000, "cost": 1.2, "current": 1.15},
            ...
        ]
    cash: 现金资产（单位：元）
    market_risk: get_market_risk_level() 返回的完整字典
    """
    # 将列表转换为字典格式（以基金代码为键）
    holdings_dict = {}
    for item in holdings_list:
        code = item.get("code")
        if code:
            holdings_dict[code] = {
                "name": item.get("name", code),
                "amount": item.get("amount", 0),
                "cost": item.get("cost", 0),
                "current": item.get("current", 0)
            }
    
    risk_level = market_risk.get('level', 'medium') if market_risk else 'medium'
    risk_msg = check_position_risk(holdings_dict)
    
    # 计算总资产和仓位比例
    total_asset = cash
    for info in holdings_dict.values():
        amount = info.get("amount", 0)
        current = info.get("current", info.get("cost", 0))
        total_asset += amount * current
    position_ratio = (total_asset - cash) / total_asset if total_asset > 0 else 0
    
    if risk_level == "high":
        position_advice = f"当前仓位 {position_ratio:.1%}，建议降低至5成以下，增持货币或债券类资产。"
    elif risk_level == "low":
        position_advice = f"当前仓位 {position_ratio:.1%}，建议可维持7成以上仓位，积极配置权益类资产。"
    else:
        position_advice = f"当前仓位 {position_ratio:.1%}，建议保持中性仓位，均衡配置。"
    
    if risk_level == "high":
        return f"【市场风险高】{risk_msg}\n{position_advice}"
    elif risk_level == "low":
        return f"【市场风险低】{risk_msg}\n{position_advice}"
    else:
        return f"【市场风险中等】{risk_msg}\n{position_advice}"
