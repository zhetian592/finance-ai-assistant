import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import atexit

logger = logging.getLogger(__name__)

SECTORS = {
    "农林牧渔": "sz.399110", "采掘": "sz.399120", "化工": "sz.399130",
    "钢铁": "sz.399140", "有色金属": "sz.399150", "电子": "sz.399160",
    "家用电器": "sz.399170", "食品饮料": "sz.399180", "纺织服装": "sz.399190",
    "医药生物": "sz.399200", "公用事业": "sz.399210", "交通运输": "sz.399220",
    "房地产": "sz.399230", "商业贸易": "sz.399240", "休闲服务": "sz.399250",
    "计算机": "sz.399260", "传媒": "sz.399270", "通信": "sz.399280",
    "国防军工": "sz.399290", "银行": "sh.000134", "非银金融": "sz.399310",
    "汽车": "sz.399320", "机械设备": "sz.399330", "建筑装饰": "sz.399340",
    "电气设备": "sz.399350", "轻工制造": "sz.399360", "建筑材料": "sz.399370",
    "综合": "sz.399380",
}

_baostock_logged_in = False

def _login_baostock():
    global _baostock_logged_in
    if not _baostock_logged_in:
        lg = bs.login()
        if lg.error_code == '0':
            _baostock_logged_in = True
            logger.info("Baostock login success")
        else:
            logger.error(f"Baostock login failed: {lg.error_msg}")
            return False
    return True

atexit.register(lambda: bs.logout() if _baostock_logged_in else None)

def get_sector_valuation(sector_name: str, lookback_years: int = 5):
    """返回 (得分, 有效标志)，若无法计算返回 (None, False)"""
    code = SECTORS.get(sector_name)
    if not code or not _login_baostock():
        return None, False
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=lookback_years*365)).strftime('%Y-%m-%d')
    try:
        rs = bs.query_history_k_data_plus(code, "date,peTTM", start, end, "d", "3")
        if rs.error_code != '0':
            return None, False
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if len(data) < 100:
            return None, False
        df = pd.DataFrame(data, columns=rs.fields)
        df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
        df = df.dropna()
        if df.empty:
            return None, False
        cur_pe = df.iloc[-1]['peTTM']
        percentile = (df['peTTM'] < cur_pe).mean() * 100
        score = 100 - percentile
        logger.info(f"[估值] {sector_name}: PE={cur_pe:.2f}, 分位={percentile:.1f}%, 得分={score:.1f}")
        return score, True
    except Exception as e:
        logger.warning(f"估值计算失败 {sector_name}: {e}")
        return None, False

def get_sector_money_flow(sector_name: str):
    try:
        df = ak.stock_hsgt_industry_em()
        if df.empty:
            return None, False
        matched = df[df['行业'].str.contains(sector_name, na=False)]
        if matched.empty:
            return None, False
        net = matched['近5日净买额(万元)'].iloc[0] / 10000
        score = np.clip((net + 20) / 40 * 100, 0, 100)
        logger.info(f"[资金] {sector_name}: 净流入{net:.1f}亿, 得分={score:.1f}")
        return score, True
    except Exception as e:
        logger.warning(f"资金接口失败 {sector_name}: {e}")
        return None, False

def get_sector_momentum(sector_name: str, days: int = 20):
    code = SECTORS.get(sector_name)
    if not code or not _login_baostock():
        return None, False
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days+5)).strftime('%Y-%m-%d')
    try:
        rs = bs.query_history_k_data_plus(code, "date,close", start, end, "d", "2")
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if len(data) < days+1:
            return None, False
        df = pd.DataFrame(data, columns=rs.fields)
        df['close'] = pd.to_numeric(df['close'])
        df = df.sort_values('date')
        ret = (df['close'].iloc[-1] - df['close'].iloc[-days-1]) / df['close'].iloc[-days-1]
        score = np.clip((ret + 0.1) / 0.3 * 100, 0, 100)
        logger.info(f"[动量] {sector_name}: {days}日收益{ret:.2%}, 得分={score:.1f}")
        return score, True
    except Exception as e:
        logger.warning(f"动量计算失败 {sector_name}: {e}")
        return None, False

def get_sector_sentiment(sector_name: str, news_events: list):
    if not news_events:
        return None, False
    # 简化：根据新闻主题匹配行业
    scores = []
    for evt in news_events:
        if sector_name in evt.get('topics', []):
            sent = evt.get('sentiment', 'neutral')
            s = evt.get('sentiment_score', 0.5)
            if sent == 'positive':
                scores.append(s * 100)
            elif sent == 'negative':
                scores.append((1 - s) * 100)
            else:
                scores.append(50)
    if not scores:
        return None, False
    score = np.mean(scores)
    logger.info(f"[情绪] {sector_name}: 相关新闻{len(scores)}条, 得分={score:.1f}")
    return score, True

def fetch_all_sector_data(sector_list: list, news_events: list) -> pd.DataFrame:
    """返回DataFrame，包含每个因子的得分和有效标志"""
    rows = []
    for sector in sector_list:
        val, val_ok = get_sector_valuation(sector)
        money, money_ok = get_sector_money_flow(sector)
        mom, mom_ok = get_sector_momentum(sector)
        sent, sent_ok = get_sector_sentiment(sector, news_events)
        
        # 有效因子列表及得分
        scores = []
        if val_ok:
            scores.append(val)
        if money_ok:
            scores.append(money)
        if mom_ok:
            scores.append(mom)
        if sent_ok:
            scores.append(sent)
        
        if scores:
            total_score = np.mean(scores)
            effective_count = len(scores)
        else:
            total_score = 50.0
            effective_count = 0
        
        rows.append({
            "sector": sector,
            "valuation_score": val if val_ok else None,
            "money_flow_score": money if money_ok else None,
            "momentum_score": mom if mom_ok else None,
            "sentiment_score": sent if sent_ok else None,
            "total_score": total_score,
            "effective_count": effective_count
        })
    df = pd.DataFrame(rows)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df
