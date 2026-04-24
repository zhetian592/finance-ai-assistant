import baostock as bs
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import atexit
import akshare as ak

logger = logging.getLogger(__name__)

SECTOR_BAOSTOCK_MAP = {
    '食品饮料': 'sh.000807', '医药生物': 'sh.000808', '银行': 'sh.000134',
    '电子': 'sh.000066',  '计算机': 'sh.000068', '建筑材料': 'sh.000150',
    '轻工制造': 'sh.000159', '非银金融': 'sh.000849', '汽车': 'sh.000941',
    '机械设备': 'sh.000109', '电气设备': 'sh.000106',
}

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

def get_sector_valuation(sector_name: str):
    """获取 PE-TTM 历史分位 (0~100)，主源 baostock，备用源 akshare"""
    percentile = _get_valuation_from_baostock(sector_name)
    if percentile is not None:
        return percentile

    logger.warning(f"{sector_name} baostock估值失败，切换备用源")
    percentile = _get_valuation_from_akshare(sector_name)
    if percentile is not None:
        return percentile

    logger.error(f"{sector_name} 所有估值源均失败")
    return None

def _get_valuation_from_baostock(sector_name: str):
    code = SECTOR_BAOSTOCK_MAP.get(sector_name)
    if not code:
        return None
    if not _login_bs():
        return None
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
    df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
    df = df.dropna()
    if df.empty:
        return None
    current_pe = df.iloc[-1]['peTTM']
    if current_pe <= 0:
        return None
    percentile = (df['peTTM'] < current_pe).mean() * 100
    logger.info(f"[Baostock] {sector_name} PE分位: {percentile:.2f}%")
    return percentile

def _get_valuation_from_akshare(sector_name: str):
    """备用源：东方财富行业板块历史日行情（无需额外参数）"""
    em_map = {
        '食品饮料': '食品饮料', '医药生物': '医药生物', '银行': '银行',
        '电子': '电子', '计算机': '计算机', '建筑材料': '建筑材料',
        '轻工制造': '轻工制造', '非银金融': '非银金融',
        '汽车': '汽车', '机械设备': '机械设备', '电气设备': '电气设备'
    }
    em_name = em_map.get(sector_name)
    if not em_name:
        return None
    try:
        df = ak.stock_board_industry_hist_em(symbol=em_name)
        if df is None or df.empty:
            return None
        # 列名：'日期'、'市盈率'等
        hist_pe = pd.to_numeric(df['市盈率'], errors='coerce').dropna()
        if hist_pe.empty:
            return None
        current_pe = hist_pe.iloc[-1]
        percentile = (hist_pe < current_pe).mean() * 100
        logger.info(f"[AKShare-EM] {sector_name} 当前PE: {current_pe:.2f}, 分位: {percentile:.2f}%")
        return percentile
    except Exception as e:
        logger.error(f"akshare备用估值失败: {e}")
        return None

def get_sector_close_series(sector_name: str, days=100):
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

def fetch_all_sector_data():
    sectors = list(SECTOR_BAOSTOCK_MAP.keys())
    result = {}
    for sec in sectors:
        val = get_sector_valuation(sec)
        close = get_sector_close_series(sec)
        result[sec] = {
            'val_percentile': val,
            'close': close
        }
    return result

def fetch_news():
    return []
