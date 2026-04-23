import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import requests
import feedparser
import logging
import time
from datetime import datetime, timedelta
import os
import atexit

logger = logging.getLogger(__name__)

# ---------- baostock 连接管理 ----------
_baostock_logged = False

def _login_bs():
    global _baostock_logged
    if not _baostock_logged:
        lg = bs.login()
        if lg.error_code == '0':
            _baostock_logged = True
        else:
            logger.error(f"Baostock login fail: {lg.error_msg}")
    return _baostock_logged

def _logout_bs():
    global _baostock_logged
    if _baostock_logged:
        bs.logout()
        _baostock_logged = False

atexit.register(_logout_bs)

# 行业 -> baostock 指数代码
SECTOR_BAOSTOCK_MAP = {
    '食品饮料': 'sh.000807', '医药生物': 'sh.000808', '银行': 'sh.000134',
    '电子': 'sh.000066', '计算机': 'sh.000068', '建筑材料': 'sh.000150',
    '轻工制造': 'sh.000159', '非银金融': 'sh.000849', '汽车': 'sh.000941',
    '机械设备': 'sh.000109', '电气设备': 'sh.000106',
}

def get_sector_valuation_baostock(sector_name):
    """返回 PE-TTM 历史分位 (0~100)，越小越低估"""
    code = SECTOR_BAOSTOCK_MAP.get(sector_name)
    if not code:
        return None
    _login_bs()
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    rs = bs.query_history_k_data_plus(code, "date,peTTM", start, end, "d", adjustflag="3")
    if rs.error_code != '0':
        return None
    data = []
    while rs.next():
        data.append(rs.get_row_data())
    if not data:
        return None
    df = pd.DataFrame(data, columns=['date', 'peTTM'])
    df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce').dropna()
    if df.empty:
        return None
    cur_pe = df.iloc[-1]['peTTM']
    percentile = (df['peTTM'] < cur_pe).mean() * 100
    return percentile

# ---------- 资金流（优先 Tushare，不可用则返回 None） ----------
try:
    import tushare as ts
    ts_token = os.getenv('TUSHARE_TOKEN')
    if ts_token:
        ts.set_token(ts_token)
        ts_pro = ts.pro_api()
    else:
        ts_pro = None
except Exception:
    ts_pro = None

def get_sector_money_flow(sector_name):
    if ts_pro is None:
        return None
    # 行业名称映射（可根据 Tushare 行业分类补充）
    mapping = {'建筑材料': '建筑材料', '银行': '银行', '电子': '电子'}
    ind = mapping.get(sector_name)
    if not ind:
        return None
    try:
        df = ts_pro.moneyflow_ind_dc(trade_date=datetime.now().strftime('%Y%m%d'),
                                    industry=ind)
        if df.empty:
            return None
        return df.iloc[0]['net_amount'] / 1e8  # 转亿
    except Exception as e:
        logger.debug(f"资金流获取失败: {e}")
        return None

# ---------- 新闻抓取（中英文混合） ----------
NEWS_SOURCES = [
    ('http://feeds.bbci.co.uk/news/business/rss.xml', '英文'),
    # 可添加：华尔街见闻、财联社、证券时报等 RSS
]

def fetch_news():
    headlines = []
    for url, lang in NEWS_SOURCES:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:10]:
                headlines.append((entry.title, url, lang))
        except Exception as e:
            logger.warning(f"新闻源 {url} 抓取失败: {e}")
    return headlines

# ---------- 统一数据获取入口 ----------
def fetch_all_sector_data():
    sectors = list(SECTOR_BAOSTOCK_MAP.keys())
    result = {}
    for sec in sectors:
        val_percentile = None
        # 先试 akshare，失败降级 baostock
        try:
            # akshare 接口频繁变动，可根据实际调整
            df_ak = ak.stock_sector_pe_ratio(sector=sec)
            if df_ak is not None and not df_ak.empty:
                # 假设 df_ak 包含历史 pe 列，计算分位
                current_pe = df_ak.iloc[-1]['pe']
                percentile = (df_ak['pe'] < current_pe).mean() * 100
                val_percentile = percentile
        except Exception:
            pass
        if val_percentile is None:
            val_percentile = get_sector_valuation_baostock(sec)
        # 资金流
        money_flow = get_sector_money_flow(sec)
        # 近期价格序列（用于动量）
        close_series = None
        code = SECTOR_BAOSTOCK_MAP.get(sec)
        if code:
            _login_bs()
            end = datetime.now().strftime('%Y-%m-%d')
            start = (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d')
            rs = bs.query_history_k_data_plus(code, "date,close", start, end, "d", adjustflag="2")
            if rs.error_code == '0':
                data = []
                while rs.next():
                    data.append(rs.get_row_data())
                if data:
                    df_close = pd.DataFrame(data, columns=['date','close'])
                    df_close['close'] = pd.to_numeric(df_close['close'])
                    close_series = df_close['close']
        result[sec] = {
            'val_percentile': val_percentile if val_percentile is not None else 50.0,
            'money_flow': money_flow if money_flow is not None else 0.0,
            'close': close_series
        }
    return result
