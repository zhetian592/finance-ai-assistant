import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import requests
import feedparser
import logging
import time
import os
from functools import lru_cache
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# -------------------- Baostock 降级模块 --------------------
_baostock_logged = False
def _login_bs():
    global _baostock_logged
    if not _baostock_logged:
        lg = bs.login()
        if lg.error_code == '0':
            _baostock_logged = True
        else:
            logger.error(f"baostock login fail: {lg.error_msg}")
    return _baostock_logged

def _logout_bs():
    global _baostock_logged
    if _baostock_logged:
        bs.logout()
        _baostock_logged = False

import atexit
atexit.register(_logout_bs)

# 行业→中证指数映射（用于baostock估值）
SECTOR_BAOSTOCK_MAP = {
    '食品饮料': 'sh.000807',
    '医药生物': 'sh.000808',
    '银行': 'sh.000134',
    '电子': 'sh.000066',
    '计算机': 'sh.000068',
    '建筑材料': 'sh.000150',
    '轻工制造': 'sh.000159',
    '非银金融': 'sh.000849',
    '汽车': 'sh.000941',
    '机械设备': 'sh.000109',
    '电气设备': 'sh.000106',
}

def get_sector_valuation_baostock(sector_name):
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
    df = pd.DataFrame(data, columns=['date','peTTM'])
    df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
    df = df.dropna()
    if df.empty:
        return None
    cur_pe = df.iloc[-1]['peTTM']
    percentile = (df['peTTM'] < cur_pe).mean() * 100
    return percentile

# -------------------- 资金流（修复：用Tushare替代AKShare失效接口） --------------------
try:
    import tushare as ts
    ts.set_token(os.getenv('TUSHARE_TOKEN'))
    ts_pro = ts.pro_api()
except:
    ts_pro = None

def get_sector_money_flow(sector_name):
    """
    获取行业资金流向。优先用Tushare moneyflow_ind_dc，若不可用则返回None。
    """
    if ts_pro is None:
        logger.warning("Tushare不可用，跳过资金流")
        return None
    try:
        # 需要映射行业名称到Tushare行业
        mapping = {
            '建筑材料': '建筑材料',
            '银行': '银行',
            '电子': '电子',
            # ... 补全其他
        }
        ind = mapping.get(sector_name)
        if not ind:
            return None
        df = ts_pro.moneyflow_ind_dc(trade_date=datetime.now().strftime('%Y%m%d'), industry=ind)
        if df.empty:
            return None
        # 返回净流入（亿）
        return df.iloc[0]['net_amount'] / 1e8
    except Exception as e:
        logger.error(f"资金流获取失败: {e}")
        return None

# -------------------- 新闻抓取（中英文混合） --------------------
NEWS_SOURCES = [
    ('https://rss.cnfol.com/finance.xml', '中文'),
    ('https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=7.7.3', '中文'),  # 财联社接口示例
    ('http://feeds.bbci.co.uk/news/business/rss.xml', '英文'),
    # 其他源...
]

def fetch_news():
    """返回 [(标题, 来源, 语言), ...]"""
    headlines = []
    for url, lang in NEWS_SOURCES:
        try:
            if 'rss' in url or 'feed' in url:
                feed = feedparser.parse(url)
                for entry in feed.entries[:10]:
                    headlines.append((entry.title, url, lang))
            else:
                # 简易json接口
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    # 根据具体格式解析，此处略
        except:
            continue
    return headlines

# -------------------- 统一数据获取入口 --------------------
def fetch_all_sector_data():
    """
    返回 dict，包含每个行业的因子原始数据：
        - valuation_percentile
        - money_flow
        - close_prices (用于动量)
    """
    # 先用akshare快速获取所有行业当日估值（如果可用），失败降级baostock
    sectors = list(SECTOR_BAOSTOCK_MAP.keys())
    result = {}
    for sec in sectors:
        # 估值
        try:
            df = ak.stock_sector_pe_ratio(sector=sec)  # 若接口存在
            val_percentile = ...  # 计算分位
        except:
            val_percentile = get_sector_valuation_baostock(sec)
        # 资金
        money = get_sector_money_flow(sec)
        # 价格（用于动量，可从baostock拉取近日收盘）
        # 简化：从baostock获取最近30个交易日close
        _login_bs()
        code = SECTOR_BAOSTOCK_MAP.get(sec)
        close_series = None
        if code:
            end = datetime.now().strftime('%Y-%m-%d')
            start = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
            rs = bs.query_history_k_data_plus(code, "date,close", start, end, "d", adjustflag="2")
            if rs.error_code == '0':
                data = []
                while rs.next():
                    data.append(rs.get_row_data())
                if data:
                    df = pd.DataFrame(data, columns=['date','close'])
                    df['close'] = pd.to_numeric(df['close'])
                    close_series = df['close']
        result[sec] = {
            'val_percentile': val_percentile,
            'money_flow': money if money is not None else 0,
            'close': close_series
        }
    return result
