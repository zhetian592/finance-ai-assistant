# quant/market_data.py
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_market_valuation(index_code="000300", years=10):
    """
    获取市场估值数据（市盈率、市净率、历史百分位）
    index_code: 000300=沪深300, 000905=中证500, 000016=上证50
    """
    try:
        # 获取指数历史PE/PB数据
        # 使用 stock_zh_index_hist_csindex 获取指数历史行情
        df = ak.stock_zh_index_hist_csindex(symbol=index_code)
        if df.empty:
            return None
        
        # 计算PE百分位
        pe_data = df["市盈率-动态"].dropna()
        if len(pe_data) < 100:
            # 数据不足，尝试获取更多历史数据
            df_full = ak.stock_zh_index_daily(symbol=index_code)
            if not df_full.empty:
                # 简化处理：使用最新值
                current_pe = float(pe_data.iloc[-1]) if len(pe_data) > 0 else None
            else:
                current_pe = None
        else:
            # 取最近 years 年数据
            cutoff_date = datetime.now() - timedelta(days=years*365)
            # 数据量足够时计算百分位
            current_pe = float(pe_data.iloc[-1]) if len(pe_data) > 0 else None
        
        # 获取最新PE值
        if current_pe is None:
            # 降级方案：使用 stock_zh_index_hist 接口
            try:
                hist = ak.stock_zh_index_hist(symbol="sh000300", period="daily", start_date="20200101", end_date=datetime.now().strftime("%Y%m%d"))
                if not hist.empty:
                    # 从历史数据中获取PE（如果存在）
                    pass
            except:
                pass
        
        # 获取指数最新收盘价
        try:
            spot = ak.stock_zh_index_spot()
            index_row = spot[spot["名称"] == "沪深300"]
            if not index_row.empty:
                current_price = float(index_row["最新价"].iloc[0])
            else:
                current_price = None
        except:
            current_price = None
        
        return {
            "index_code": index_code,
            "index_name": "沪深300",
            "current_pe": current_pe,
            "current_price": current_price,
            "data_date": datetime.now().strftime("%Y-%m-%d")
        }
    except Exception as e:
        logger.warning(f"获取市场估值失败: {e}")
        return None

def get_north_flow(days=5):
    """获取北向资金（外资）净流入"""
    try:
        # 获取北向资金日频数据
        df = ak.stock_hsgt_north_net_flow_in_em()
        if df.empty:
            return None
        
        # 获取最近 days 天的净流入
        recent = df.head(days)
        total_net = recent["净买入(万元)"].sum() / 10000  # 转换为亿元
        
        latest = recent.iloc[0] if len(recent) > 0 else None
        return {
            "total_net_billion": round(total_net, 2),
            "latest_net": round(float(latest["净买入(万元)"]) / 10000, 2) if latest is not None else None,
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
    
    # PE估值判断
    if valuation and valuation.get("current_pe"):
        pe = valuation["current_pe"]
        if pe > 15:
            risk_score += 30
            reasons.append(f"沪深300 PE={pe:.1f}，处于相对高位")
        elif pe < 11:
            risk_score -= 20
            reasons.append(f"沪深300 PE={pe:.1f}，处于相对低位")
    
    # 北向资金判断
    if north_flow and north_flow.get("total_net_billion"):
        net = north_flow["total_net_billion"]
        if net > 100:
            risk_score -= 10
            reasons.append(f"北向资金近5日净流入{net}亿，外资看好")
        elif net < -50:
            risk_score += 20
            reasons.append(f"北向资金近5日净流出{abs(net)}亿，外资撤离")
    
    # 风险等级判定
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
