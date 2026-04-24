import baostock as bs
import pandas as pd
import logging
from datetime import datetime, timedelta
import atexit

logger = logging.getLogger(__name__)

SECTOR_BAOSTOCK_MAP = {
    '食品饮料': 'sh.000807', '医药生物': 'sh.000808', '银行': 'sh.000134',
    '电子': 'sh.000066', '计算机': 'sh.000068', '建筑材料': 'sh.000150',
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
    """返回 PE-TTM 历史分位 (0~100)，越低越低估"""
    code = SECTOR_BAOSTOCK_MAP.get(sector_name)
    if not code:
        return None

    if not _login_bs():
        return None

    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    rs = bs.query_history_k_data_plus(code, "date,peTTM", start, end, "d", adjustflag="3")
    if rs.error_code != '0':
        logger.warning(f"{sector_name} 查询失败: {rs.error_msg}")
        return None

    data = []
    while rs.next():
        data.append(rs.get_row_data())
    if not data:
        logger.warning(f"{sector_name} 返回空数据")
        return None

    df = pd.DataFrame(data, columns=['date', 'peTTM'])
    df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
    df = df.dropna()

    if df.empty:
        logger.warning(f"{sector_name} PE数据全部为NaN")
        return None

    current_pe = df.iloc[-1]['peTTM']
    if current_pe <= 0:
        logger.warning(f"{sector_name} 当前PE异常: {current_pe}")
        return None

    percentile = (df['peTTM'] < current_pe).mean() * 100
    logger.info(f"{sector_name} PE分位: {percentile:.1f}%")
    return percentile

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

def fetch_all_sector_data():
    """返回 {sector: {'val_percentile': float|None, 'close': Series|None}}"""
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
    """
    新闻抓取。当前返回空列表占位。
    后续可接入中文RSS源（财联社、华尔街见闻等）。
    """
    return []
