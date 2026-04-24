import baostock as bs
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import atexit
import akshare as ak  # 备用估值源

logger = logging.getLogger(__name__)

# ---------- 行业名称 -> baostock 中证指数代码 ----------
SECTOR_BAOSTOCK_MAP = {
    '食品饮料': 'sh.000807', '医药生物': 'sh.000808', '银行': 'sh.000134',
    '电子': 'sh.000066',  '计算机': 'sh.000068', '建筑材料': 'sh.000150',
    '轻工制造': 'sh.000159', '非银金融': 'sh.000849', '汽车': 'sh.000941',
    '机械设备': 'sh.000109', '电气设备': 'sh.000106',
}

# ---------- baostock 连接管理 ----------
_baostock_logged = False

def _login_bs():
    global _baostock_logged
    if not _baostock_logged:
        lg = bs.login()
        if lg.error_code == '0':
            _baostock_logged = True
            logger.info("Baostock login success")
        else:
            logger.error(f"Baostock login fail: {lg.error_msg}")
    return _baostock_logged

def _logout_bs():
    global _baostock_logged
    if _baostock_logged:
        bs.logout()
        _baostock_logged = False

atexit.register(_logout_bs)

# ---------- 估值获取（主备逻辑） ----------
def get_sector_valuation(sector_name: str):
    """
    获取行业 PE-TTM 历史分位 (0~100)，越小越低估。
    主源：baostock，备用源：akshare。
    """
    # 1. 主源 baostock
    percentile = _get_valuation_from_baostock(sector_name)
    if percentile is not None:
        return percentile

    logger.warning(f"{sector_name} baostock估值失败，切换备用源 akshare")
    # 2. 备用源 akshare
    percentile = _get_valuation_from_akshare(sector_name)
    if percentile is not None:
        return percentile

    logger.error(f"{sector_name} 所有估值源均失败，返回 None")
    return None

def _get_valuation_from_baostock(sector_name: str):
    """原有的 baostock 估值逻辑（数据异常时返回 None）"""
    code = SECTOR_BAOSTOCK_MAP.get(sector_name)
    if not code:
        return None

    if not _login_bs():
        return None

    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    rs = bs.query_history_k_data_plus(code, "date,peTTM", start, end, "d", adjustflag="3")
    if rs.error_code != '0':
        logger.debug(f"{sector_name} baostock查询错误: {rs.error_msg}")
        return None

    data = []
    while rs.next():
        data.append(rs.get_row_data())
    if not data:
        logger.debug(f"{sector_name} baostock无数据")
        return None

    df = pd.DataFrame(data, columns=['date', 'peTTM'])
    df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
    df = df.dropna()

    if df.empty:
        logger.debug(f"{sector_name} baostock PE全为NaN")
        return None

    current_pe = df.iloc[-1]['peTTM']
    if current_pe <= 0:
        logger.debug(f"{sector_name} baostock当前PE异常: {current_pe}")
        return None

    percentile = (df['peTTM'] < current_pe).mean() * 100
    logger.info(f"[Baostock] {sector_name} PE分位: {percentile:.2f}%")
    return percentile

def _get_valuation_from_akshare(sector_name: str):
    """备用估值源：akshare 申万行业PE分位"""
    try:
        df = ak.stock_sector_pe_ratio(sector=sector_name)
        if df is None or df.empty:
            logger.warning(f"{sector_name} akshare返回空")
            return None

        # 接口列名可能为“市盈率-动态”或类似，取最后一列数值列
        pe_col = None
        for col in ['市盈率-动态', '动态市盈率', 'pe']:
            if col in df.columns:
                pe_col = col
                break
        if pe_col is None:
            # 尝试除了'date'以外的第一个数值列
            for col in df.columns:
                if col != 'date':
                    pe_col = col
                    break
        if pe_col is None:
            return None

        hist_pe = pd.to_numeric(df[pe_col], errors='coerce').dropna()
        if hist_pe.empty:
            return None

        current_pe = hist_pe.iloc[-1]
        percentile = (hist_pe < current_pe).mean() * 100
        logger.info(f"[AKShare] {sector_name} 当前PE: {current_pe:.2f}, 分位: {percentile:.2f}%")
        return percentile
    except Exception as e:
        logger.error(f"akshare备用估值失败: {e}")
        return None

# ---------- 价格序列获取（用于动量计算） ----------
def get_sector_close_series(sector_name: str, days=100):
    """获取行业指数最近 days 天的收盘价 Series"""
    code = SECTOR_BAOSTOCK_MAP.get(sector_name)
    if not code or not _login_bs():
        return None

    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    rs = bs.query_history_k_data_plus(code, "date,close", start, end, "d", adjustflag="2")
    if rs.error_code != '0':
        return None

    data = []
    while rs.next():
        data.append(rs.get_row_data())
    if not data:
        return None

    df = pd.DataFrame(data, columns=['date', 'close'])
    df['close'] = pd.to_numeric(df['close'])
    return df['close']

# ---------- 统一数据获取入口 ----------
def fetch_all_sector_data():
    """返回 dict：{sector: {'val_percentile': float|None, 'close': Series|None}}"""
    sectors = list(SECTOR_BAOSTOCK_MAP.keys())
    result = {}
    for sec in sectors:
        val = get_sector_valuation(sec)          # 自动主备降级
        close = get_sector_close_series(sec)     # baostock 价格
        result[sec] = {
            'val_percentile': val,
            'close': close
        }
    return result

# ---------- 新闻抓取（占位，后续扩展） ----------
def fetch_news():
    """当前返回空列表，后续接入中文财经RSS"""
    return []
