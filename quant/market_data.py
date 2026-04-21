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
        # 使用股票指数行情接口获取最新点位
        # 注意：AKShare 接口会变化，这里使用较稳定的 spot 接口
        spot_df = ak.stock_zh_index_spot()
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
        # 注意：stock_zh_index_pe 可能不稳定，这里简单处理
        current_pe = None
        try:
            pe_df = ak.stock_zh_index_pe(symbol=f"sh{index_code}")
            if not pe_df.empty:
                current_pe = float(pe_df.iloc[-1]["pe"])
        except:
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
        df = ak.stock_hsgt_north_net_flow_in_em()
        if df.empty:
            return None
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
    
    if valuation and valuation.get("current_pe"):
        pe = valuation["current_pe"]
        if pe > 15:
            risk_score += 30
            reasons.append(f"沪深300 PE={pe:.1f}，处于相对高位")
        elif pe < 11:
            risk_score -= 20
            reasons.append(f"沪深300 PE={pe:.1f}，处于相对低位")
    
    if north_flow and north_flow.get("total_net_billion"):
        net = north_flow["total_net_billion"]
        if net > 100:
            risk_score -= 10
            reasons.append(f"北向资金近5日净流入{net}亿，外资看好")
        elif net < -50:
            risk_score += 20
            reasons.append(f"北向资金近5日净流出{abs(net)}亿，外资撤离")
    
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
