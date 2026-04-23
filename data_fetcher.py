import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import logging
import time
from functools import lru_cache
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# -------------------- 行业配置 --------------------
# 申万一级行业 → baostock 中证行业指数代码（部分映射，可扩展）
SECTORS = {
    "农林牧渔": "sz.399110",
    "采掘": "sz.399120",
    "化工": "sz.399130",
    "钢铁": "sz.399140",
    "有色金属": "sz.399150",
    "电子": "sz.399160",
    "家用电器": "sz.399170",
    "食品饮料": "sz.399180",
    "纺织服装": "sz.399190",
    "医药生物": "sz.399200",
    "公用事业": "sz.399210",
    "交通运输": "sz.399220",
    "房地产": "sz.399230",
    "商业贸易": "sz.399240",
    "休闲服务": "sz.399250",
    "计算机": "sz.399260",
    "传媒": "sz.399270",
    "通信": "sz.399280",
    "国防军工": "sz.399290",
    "银行": "sh.000134",
    "非银金融": "sz.399310",
    "汽车": "sz.399320",
    "机械设备": "sz.399330",
    "建筑装饰": "sz.399340",
    "电气设备": "sz.399350",
    "轻工制造": "sz.399360",
    "建筑材料": "sz.399370",
    "综合": "sz.399380",
}

# -------------------- baostock 全局连接管理 --------------------
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

def _logout_baostock():
    global _baostock_logged_in
    if _baostock_logged_in:
        bs.logout()
        _baostock_logged_in = False

import atexit
atexit.register(_logout_baostock)

# -------------------- 估值因子（baostock 降级） --------------------
def get_sector_valuation_baostock(sector_name: str, lookback_years: int = 3):
    """使用 baostock 获取行业指数 PE-TTM 历史分位"""
    code = SECTORS.get(sector_name)
    if code is None:
        return None
    if not _login_baostock():
        return None
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=lookback_years*365)).strftime('%Y-%m-%d')
    rs = bs.query_history_k_data_plus(
        code=code,
        fields="date,peTTM",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3"
    )
    if rs.error_code != '0':
        logger.error(f"Baostock query error: {rs.error_msg}")
        return None
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list:
        return None
    df = pd.DataFrame(data_list, columns=rs.fields)
    df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
    df = df.dropna(subset=['peTTM'])
    if df.empty:
        return None
    current_pe = df.iloc[-1]['peTTM']
    percentile = (df['peTTM'] < current_pe).mean() * 100
    # 估值得分 = 100 - 分位（越低越得分）
    score = 100 - percentile
    logger.info(f"[Baostock] {sector_name} 当前PE: {current_pe:.2f}, 历史分位: {percentile:.1f}%, 得分: {score:.1f}")
    return score

def get_sector_valuation(sector_name: str):
    """优先 akshare，失败降级 baostock"""
    try:
        # 尝试 akshare 获取行业估值（需根据实际接口调整）
        # 示例：ak.stock_sector_pe_ratio 可能不存在，这里直接使用 baostock 更稳定
        # 为简化，直接使用 baostock
        raise Exception("AKShare 接口不稳定，直接使用 baostock")
    except Exception as e:
        logger.debug(f"AKShare 估值获取失败: {e}, 降级 baostock")
        return get_sector_valuation_baostock(sector_name)

# -------------------- 资金流因子 --------------------
def get_sector_money_flow(sector_name: str) -> float:
    """获取北向资金近5日净流入（亿元），返回得分0~100"""
    try:
        df = ak.stock_hsgt_industry_em()
        if df.empty:
            return 50.0
        # 行业名称可能不完全匹配，做模糊匹配
        row = df[df['行业'].str.contains(sector_name, na=False)]
        if row.empty:
            return 50.0
        # 取近5日净买额（万元）转为亿元
        net = row['近5日净买额(万元)'].iloc[0] / 10000
        # 线性映射：净流入 >20亿 -> 100分，<-20亿 -> 0分
        score = (net + 20) / 40 * 100
        return np.clip(score, 0, 100)
    except Exception as e:
        logger.warning(f"获取北向资金 {sector_name} 失败: {e}")
        return 50.0

# -------------------- 动量因子 --------------------
def get_sector_momentum(sector_name: str, days: int = 20) -> float:
    """获取行业指数最近 days 日收益率，返回得分0~100"""
    code = SECTORS.get(sector_name)
    if code is None:
        return 50.0
    if not _login_baostock():
        return 50.0
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days+5)).strftime('%Y-%m-%d')
    rs = bs.query_history_k_data_plus(
        code=code,
        fields="date,close",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2"
    )
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if len(data_list) < days+1:
        return 50.0
    df = pd.DataFrame(data_list, columns=rs.fields)
    df['close'] = pd.to_numeric(df['close'])
    df = df.sort_values('date')
    ret = (df['close'].iloc[-1] - df['close'].iloc[-days-1]) / df['close'].iloc[-days-1]
    # 假设收益率范围 -0.1 ~ 0.2，映射到0~100
    score = (ret + 0.1) / 0.3 * 100
    return np.clip(score, 0, 100)

# -------------------- 情绪因子（基于新闻事件） --------------------
def get_sector_sentiment(sector_name: str, news_events: list) -> float:
    """根据新闻事件计算行业情绪得分"""
    if not news_events:
        return 50.0
    scores = []
    for evt in news_events:
        topics = evt.get('topics', [])
        if any(topic in sector_name or sector_name in topic for topic in topics):
            sentiment = evt.get('sentiment', 'neutral')
            sent_score = evt.get('sentiment_score', 0.5)
            if sentiment == 'positive':
                scores.append(sent_score * 100)
            elif sentiment == 'negative':
                scores.append((1 - sent_score) * 100)
            else:
                scores.append(50)
    if not scores:
        return 50.0
    return np.mean(scores)

# -------------------- 综合数据获取 --------------------
def fetch_all_sector_data(sector_list: list, news_events: list) -> pd.DataFrame:
    """获取所有行业的多因子数据，返回 DataFrame"""
    data = []
    for sector in sector_list:
        val_score = get_sector_valuation(sector)
        if val_score is None:
            val_score = 50.0
        money_score = get_sector_money_flow(sector)
        mom_score = get_sector_momentum(sector)
        sent_score = get_sector_sentiment(sector, news_events)
        total_score = (val_score + money_score + mom_score + sent_score) / 4
        data.append({
            "sector": sector,
            "etf_code": "",  # 可后续映射真实ETF代码
            "valuation_score": val_score,
            "money_flow_score": money_score,
            "momentum_score": mom_score,
            "sentiment_score": sent_score,
            "total_score": total_score
        })
    df = pd.DataFrame(data)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df
