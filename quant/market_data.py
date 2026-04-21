# quant/market_data.py
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_market_valuation(index_code="000300", years=10):
    """
    获取市场估值数据
    index_code: 000300=沪深300, 000905=中证500, 000016=上证50
    """
    try:
        # 使用新接口 stock_zh_index_spot_em 获取指数实时行情
        spot_df = ak.stock_zh_index_spot_em()
        if spot_df.empty:
            return None

        # 查找对应指数
        index_name_map = {
            "000300": "沪深300",
            "000905": "中证500", 
            "000016": "上证50"
        }
        name = index_name_map.get(index_code, "沪深300")
        row = spot_df[spot_df["名称"] == name]
        if row.empty:
            return None
        
        current_price = float(row["最新价"].iloc[0])
        
        # 获取历史PE数据（可选，如果接口可用）
        current_pe = None
        try:
            # 使用新接口 stock_zh_index_hist_em 获取历史数据计算PE
            hist_df = ak.stock_zh_index_hist_em(symbol=f"sh{index_code}", period="daily", start_date="20240101", end_date=datetime.now().strftime("%Y%m%d"))
            if not hist_df.empty:
                # 注意：历史数据接口通常不直接提供PE，这里为示例逻辑，实际可能需要其他接口
                # 为了简化，我们暂时忽略PE获取，或可以留作扩展
                # 如果有PE数据，可以尝试从其他接口获取，如 stock_a_pe_and_pb
                pass
        except Exception as e:
            logger.debug(f"获取PE数据失败: {e}")
            pass

        return {
            "index_code": index_code,
            "index_name": name,
            "current_pe": current_pe,
            "current_price": current_price,
            "data_date": datetime.now().strftime("%Y-%m-%d")
        }
    except Exception as e:
        logger.warning(f"获取市场估值失败: {e}")
        return None

def get_north_flow(days=5):
    """获取北向资金净流入"""
    try:
        # 使用新接口 stock_hsgt_hist_em 获取北向资金历史数据
        df = ak.stock_hsgt_hist_em()
        if df.empty:
            return None
        
        # 计算最近N日净流入总额（单位：亿元）
        # 注意：数据中“净买入”列的单位为万元
        recent = df.head(days)
        total_net = recent["净买入(万元)"].sum() / 10000
        
        return {
            "total_net_billion": round(total_net, 2),
            "data_date": datetime.now().strftime("%Y-%m-%d")
        }
    except Exception as e:
        logger.warning(f"获取北向资金失败: {e}")
        return None

def get_market_risk_level():
    """综合判断市场风险等级"""
    valuation = get_market_valuation()
    north_flow = get_north_flow()
    
    risk_score = 0
    reasons = []
    
    # 根据估值评分
    if valuation and valuation.get("current_price"):
        price = valuation["current_price"]
        # 简单示例：根据沪深300点位粗略判断估值高低（实际应使用PE百分位）
        if price > 4500:
            risk_score += 30
            reasons.append(f"沪深300点位{price:.0f}，处于相对高位")
        elif price < 3500:
            risk_score -= 20
            reasons.append(f"沪深300点位{price:.0f}，处于相对低位")
    
    # 根据北向资金评分
    if north_flow and north_flow.get("total_net_billion"):
        net = north_flow["total_net_billion"]
        if net > 100:
            risk_score -= 10
            reasons.append(f"北向资金近5日净流入{net}亿，外资看好")
        elif net < -50:
            risk_score += 20
            reasons.append(f"北向资金近5日净流出{abs(net)}亿，外资撤离")
    
    # 确定风险等级
    if risk_score >= 40:
        level = "high"
        advice = "市场风险较高，建议降低仓位或观望"
    elif risk_score >= 15:
        level = "medium"
        advice = "市场存在一定风险，控制仓位"
    else:
        level = "low"
        advice = "市场风险较低，可适当参与"
    
    return {
        "level": level,
        "score": risk_score,
        "advice": advice,
        "reasons": reasons,
        "valuation": valuation,
        "north_flow": north_flow
    }
