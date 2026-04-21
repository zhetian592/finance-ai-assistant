import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_market_valuation(index_code="000300", years=10):
    """
    获取市场估值数据（当前价格，可选PE）
    index_code: 000300=沪深300, 000905=中证500, 000016=上证50
    """
    try:
        spot_df = ak.stock_zh_index_spot_em()
        if spot_df.empty:
            return None

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
        
        # 可选：尝试获取PE（此处简化，不强制要求）
        current_pe = None
        try:
            # 使用 stock_a_pe 接口尝试获取PE（需要较新akshare版本）
            pe_df = ak.stock_a_pe()
            if not pe_df.empty and "沪深300" in pe_df["名称"].values:
                current_pe = float(pe_df[pe_df["名称"] == "沪深300"]["市盈率"].iloc[0])
        except Exception:
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
    """
    获取北向资金净流入（自动适配列名）
    """
    try:
        df = ak.stock_hsgt_hist_em()
        if df.empty:
            return None
        
        # 自动查找净买入列（支持多种列名）
        net_col = None
        for col in df.columns:
            if '净买入' in col and ('万元' in col or '万' in col):
                net_col = col
                break
        if net_col is None:
            # 常见备选列名
            candidates = ['北向资金净买入(万元)', '净买入(万元)', '净买入额(万元)', '买入成交净额(万元)']
            for cand in candidates:
                if cand in df.columns:
                    net_col = cand
                    break
        
        if net_col is None:
            # 最后尝试使用第一个数值列（不推荐，仅容错）
            numeric_cols = df.select_dtypes(include='number').columns
            if len(numeric_cols) > 0:
                net_col = numeric_cols[0]
                logger.warning(f"未找到净买入列，使用 {net_col} 代替")
            else:
                raise KeyError("无法识别北向资金净买入列")
        
        # 最近N日净流入总额（单位：亿元，原始数据通常为万元）
        recent = df.head(days)
        total_net = recent[net_col].sum() / 10000
        
        return {
            "total_net_billion": round(total_net, 2),
            "data_date": datetime.now().strftime("%Y-%m-%d")
        }
    except Exception as e:
        logger.warning(f"获取北向资金失败: {e}")
        return None

def get_market_risk_level():
    """
    综合判断市场风险等级
    返回字典包含 level (high/medium/low), score, advice, reasons, valuation, north_flow
    """
    valuation = get_market_valuation()
    north_flow = get_north_flow()
    
    risk_score = 0
    reasons = []
    
    # 估值评分（根据沪深300价格简单划分）
    if valuation and valuation.get("current_price"):
        price = valuation["current_price"]
        if price > 4500:
            risk_score += 30
            reasons.append(f"沪深300点位{price:.0f}，处于相对高位")
        elif price < 3500:
            risk_score -= 20
            reasons.append(f"沪深300点位{price:.0f}，处于相对低位")
        else:
            reasons.append(f"沪深300点位{price:.0f}，处于中等区域")
    
    # 北向资金评分
    if north_flow and north_flow.get("total_net_billion"):
        net = north_flow["total_net_billion"]
        if net > 100:
            risk_score -= 10
            reasons.append(f"北向资金近5日净流入{net}亿，外资看好")
        elif net < -50:
            risk_score += 20
            reasons.append(f"北向资金近5日净流出{abs(net)}亿，外资撤离")
        else:
            reasons.append(f"北向资金近5日净流入{net}亿，外资态度平稳")
    
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
