import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict
import time

# 延迟导入 akshare 和 baostock，避免不必要的依赖问题
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    logging.warning("akshare 未安装，行情获取将降级到 baostock")

try:
    import baostock as bs
    BAOSTOCK_AVAILABLE = True
except ImportError:
    BAOSTOCK_AVAILABLE = False
    logging.warning("baostock 未安装，行情获取将降级到模拟数据")

logger = logging.getLogger(__name__)

class DataService:
    def __init__(self, cache_dir=".cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._baostock_logged = False
        self._init_baostock()

    def _init_baostock(self):
        if BAOSTOCK_AVAILABLE and not self._baostock_logged:
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    self._baostock_logged = True
                    logger.info("Baostock login success")
                else:
                    logger.error(f"Baostock login failed: {lg.error_msg}")
            except Exception as e:
                logger.error(f"Baostock exception: {e}")

    def get_daily_ohlcv(self, symbol: str, count: int = 100) -> Optional[pd.DataFrame]:
        """获取日线 OHLCV，优先 akshare，其次 baostock，最后模拟数据"""
        # 1. akshare (最稳定)
        df = self._fetch_from_akshare(symbol, count)
        if df is not None:
            return df
        # 2. baostock (备用)
        df = self._fetch_from_baostock(symbol, count)
        if df is not None:
            return df
        # 3. 模拟数据（仅作为最后降级，打印警告）
        logger.warning(f"所有行情源失败，使用模拟数据 for {symbol}")
        return self._generate_fake_ohlcv(symbol, count)

    def _fetch_from_akshare(self, symbol: str, count: int):
        """使用 akshare 获取行业指数日线"""
        if not AKSHARE_AVAILABLE:
            return None
        try:
            # symbol 格式如 "sz.399110"，取出代码 "399110"
            code = symbol.split('.')[-1] if '.' in symbol else symbol
            # 获取历史数据（akshare 的 stock_zh_index_hist 需要指数代码）
            df = ak.stock_zh_index_hist(symbol=code, period="daily", start_date="19900101", end_date=datetime.now().strftime("%Y%m%d"))
            if df is None or df.empty:
                logger.debug(f"akshare 返回空数据 for {symbol}")
                return None
            # 标准化列名
            df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume'
            }, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            # 只取最近 count 条
            return df[['date','open','high','low','close','volume']].iloc[-count:]
        except Exception as e:
            logger.debug(f"akshare 获取失败 {symbol}: {e}")
            return None

    def _fetch_from_baostock(self, symbol: str, count: int):
        """使用 baostock 获取日线"""
        if not BAOSTOCK_AVAILABLE or not self._baostock_logged:
            return None
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=count*2)).strftime('%Y-%m-%d')
        try:
            rs = bs.query_history_k_data_plus(
                symbol,
                "date,open,high,low,close,volume",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="2"
            )
            if rs.error_code != '0':
                logger.debug(f"Baostock query error {symbol}: {rs.error_msg}")
                return None
            data = []
            while rs.next():
                data.append(rs.get_row_data())
            if not data or len(data) < count:
                return None
            df = pd.DataFrame(data, columns=['date','open','high','low','close','volume'])
            for c in ['open','high','low','close','volume']:
                df[c] = pd.to_numeric(df[c])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            return df[['date','open','high','low','close','volume']].iloc[-count:]
        except Exception as e:
            logger.debug(f"Baostock fetch error {symbol}: {e}")
            return None

    def _generate_fake_ohlcv(self, symbol: str, count: int):
        """仅用于最终降级，避免程序崩溃"""
        dates = pd.date_range(end=datetime.today(), periods=count, freq='D')
        close = np.cumsum(np.random.randn(count)) + 1000
        open_ = close + np.random.randn(count) * 2
        high = close + abs(np.random.randn(count)) * 3
        low = close - abs(np.random.randn(count)) * 3
        volume = np.random.randint(100000, 1000000, count)
        df = pd.DataFrame({'date':dates, 'open':open_, 'high':high, 'low':low, 'close':close, 'volume':volume})
        return df

    def get_valuation(self, sector: str) -> Optional[Dict]:
        csv_path = "data/valuation.csv"
        if not os.path.exists(csv_path):
            logger.debug(f"估值文件 {csv_path} 不存在")
            return None
        try:
            df = pd.read_csv(csv_path)
            sector_df = df[df['sector'] == sector].sort_values('date', ascending=False)
            if sector_df.empty:
                return None
            latest = sector_df.iloc[0]
            return {
                'pe_percentile': float(latest['pe_percentile']),
                'pb_percentile': float(latest['pb_percentile'])
            }
        except Exception as e:
            logger.error(f"读取估值失败 {sector}: {e}")
            return None
