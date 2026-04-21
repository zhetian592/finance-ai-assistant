# backtest/calculator.py
import akshare as ak
import pandas as pd
from .recorder import load_recommendations
import logging

logger = logging.getLogger(__name__)

def get_historical_nav(fund_code, date):
    """获取指定日期的基金净值"""
    try:
        df = ak.fund_open_fund_info_em(fund_code, indicator="单位净值走势")
        if df.empty:
            return None
        df["净值日期"] = pd.to_datetime(df["净值日期"])
        target = pd.to_datetime(date)
        # 找到最接近的日期
        closest_idx = (df["净值日期"] - target).abs().idxmin()
        return float(df.loc[closest_idx, "单位净值"])
    except Exception as e:
        logger.warning(f"获取历史净值失败 {fund_code} {date}: {e}")
        return None

def calculate_performance():
    """计算历史推荐的表现"""
    records = load_recommendations()
    if not records:
        return None
    
    results = []
    total_return = 0
    win_count = 0
    
    for rec in records:
        if rec["status"] != "executed":
            continue
        
        buy_price = rec.get("actual_buy_price") or rec.get("nav_at_time")
        sell_price = rec.get("sell_price")
        
        if buy_price and sell_price:
            ret = (sell_price - buy_price) / buy_price
            total_return += ret
            if ret > 0:
                win_count += 1
            results.append({
                "fund_code": rec["fund_code"],
                "action": rec["action"],
                "return": ret,
                "date": rec["date"]
            })
    
    if not results:
        return None
    
    return {
        "total_trades": len(results),
        "win_count": win_count,
        "win_rate": win_count / len(results) * 100 if results else 0,
        "total_return": total_return,
        "avg_return": total_return / len(results) if results else 0,
        "details": results
    }
