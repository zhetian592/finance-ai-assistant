# backtest/recorder.py
import json
import os
from datetime import datetime

RECORDS_FILE = "backtest/recommendations.json"

def load_recommendations():
    """加载历史推荐记录"""
    if os.path.exists(RECORDS_FILE):
        try:
            with open(RECORDS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return []

def save_recommendation(rec):
    """保存一条推荐记录"""
    records = load_recommendations()
    records.append(rec)
    with open(RECORDS_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

def record_ai_recommendation(fund_code, fund_name, action, reason, nav_at_time, amount_suggested=0):
    """记录AI给出的建议"""
    rec = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "timestamp": datetime.now().isoformat(),
        "fund_code": fund_code,
        "fund_name": fund_name,
        "action": action,  # buy/sell/hold/add
        "reason": reason,
        "nav_at_time": nav_at_time,
        "amount_suggested": amount_suggested,
        "status": "pending",  # pending/executed/ignored
        "actual_buy_price": None,
        "sell_price": None,
        "profit_loss": None
    }
    save_recommendation(rec)
    return rec
