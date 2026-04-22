# 规则引擎：根据事件主题和情感推荐基金
# 格式：主题 -> 推荐基金列表（代码，名称，权重）
FUND_RULES = {
    "新能源": [
        {"code": "516160", "name": "南方新能源ETF", "weight": 1.0},
        {"code": "001410", "name": "信达澳银新能源产业", "weight": 0.8}
    ],
    "消费": [
        {"code": "110022", "name": "易方达消费行业", "weight": 1.0},
        {"code": "260108", "name": "景顺长城新兴成长", "weight": 0.9}
    ],
    "科技": [
        {"code": "515050", "name": "华夏中证5G通信ETF", "weight": 1.0},
        {"code": "001790", "name": "国泰智能汽车", "weight": 0.7}
    ],
    "医药": [
        {"code": "512010", "name": "易方达沪深300医药ETF", "weight": 1.0},
        {"code": "003095", "name": "中欧医疗健康混合", "weight": 0.9}
    ],
    "金融": [
        {"code": "512880", "name": "国泰中证全指证券公司ETF", "weight": 1.0},
        {"code": "001594", "name": "天弘中证银行ETF联接", "weight": 0.8}
    ],
    "能源": [
        {"code": "162411", "name": "华宝油气", "weight": 1.0},
        {"code": "160416", "name": "华安标普全球石油", "weight": 0.9}
    ],
    "债券": [
        {"code": "040040", "name": "华安纯债债券A", "weight": 1.0},
        {"code": "000227", "name": "华安年年红债券", "weight": 0.9}
    ],
    "宏观": []  # 宏观事件影响整体仓位，不推荐具体基金
}

def get_recommendations_by_events(events: list, cash: float, market_risk: dict) -> list:
    """
    基于事件列表推荐基金
    返回: [{"code":..., "name":..., "amount":..., "reason":...}]
    """
    if not events:
        return []
    
    # 汇总每个主题的综合情感强度
    topic_scores = {}
    for evt in events:
        for topic in evt.get("topics", []):
            if topic not in FUND_RULES:
                continue
            sentiment = evt.get("sentiment", "neutral")
            score = evt.get("sentiment_score", 0.5)
            # 正面+0.3，负面-0.2，中性+0
            delta = 0.3 if sentiment == "positive" else (-0.2 if sentiment == "negative" else 0)
            topic_scores[topic] = topic_scores.get(topic, 0) + score + delta
    
    # 排序，取最积极的前3个主题
    sorted_topics = sorted(topic_scores.items(), key=lambda x: x[1], reverse=True)[:3]
    
    # 根据主题推荐基金
    recommendations = []
    total_amount = 0
    remaining = cash
    for topic, strength in sorted_topics:
        if strength <= 0.5:  # 情感不够强烈，跳过
            continue
        rule = FUND_RULES.get(topic, [])
        if not rule:
            continue
        # 取权重最高的基金
        best = max(rule, key=lambda x: x["weight"])
        # 分配金额：强度越高，金额比例越高（20%~40%）
        ratio = min(0.4, 0.2 + (strength - 0.5) * 0.4)
        amount = int(remaining * ratio) if remaining > 0 else 0
        if amount <= 0:
            continue
        total_amount += amount
        remaining -= amount
        recommendations.append({
            "code": best["code"],
            "name": best["name"],
            "amount": amount,
            "reason": f"事件驱动：近期{topic}板块出现{strength:.2f}强度正面信号"
        })
        if len(recommendations) >= 3:
            break
    
    # 如果现金未分配完，加到第一个推荐上
    if recommendations and remaining > 0:
        recommendations[0]["amount"] += remaining
    
    return recommendations
