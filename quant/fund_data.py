# quant/fund_data.py
import akshare as ak
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_fund_latest_nav(fund_code: str):
    """
    获取基金最新净值
    使用 fund_open_fund_daily_em 获取全市场开放式基金最新净值
    """
    try:
        # 获取全市场开放式基金最新净值
        df = ak.fund_open_fund_daily_em()
        if df.empty:
            return None
        
        # 筛选目标基金
        fund_row = df[df["基金代码"] == fund_code]
        if fund_row.empty:
            return None
        
        latest_nav = float(fund_row["单位净值"].iloc[0])
        return latest_nav
    except Exception as e:
        logger.warning(f"获取基金 {fund_code} 净值失败: {e}")
        return None

def get_fund_history_nav(fund_code: str, days=30):
    """获取基金历史净值（用于回测）"""
    try:
        df = ak.fund_open_fund_info_em(fund_code, indicator="单位净值走势")
        if df.empty:
            return None
        
        df = df.sort_values("净值日期", ascending=False)
        return df.head(days)
    except Exception as e:
        logger.warning(f"获取基金 {fund_code} 历史净值失败: {e}")
        return None

def update_fund_nav(holdings):
    """
    批量更新持仓基金净值
    返回更新后的 holdings 和是否有更新的标志
    """
    updated = False
    for h in holdings:
        fund_code = h.get("fund_code")
        if not fund_code:
            continue
        
        nav = get_fund_latest_nav(fund_code)
        if nav and nav > 0:
            old_nav = h.get("current_nav")
            if old_nav != nav:
                updated = True
            h["current_nav"] = nav
            # 计算盈亏
            cost = h.get("cost", 0)
            amount = h.get("amount", 0)
            if cost > 0:
                h["profit_loss"] = (nav - cost) * amount
                h["profit_loss_percent"] = (nav - cost) / cost * 100
                h["update_date"] = datetime.now().strftime("%Y-%m-%d")
    
    return updated
