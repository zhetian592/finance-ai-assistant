import pandas as pd
import numpy as np
import requests
import baostock as bs
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict
import os

logger = logging.getLogger(__name__)

class DataService:
    def __init__(self, cache_dir=".cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._baostock_logged = False
        self._init_baostock()

    def _init_baostock(self):
        if not self._baostock_logged:
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    self._baostock_logged = True
                    logger.info("Baostock login success")
            except:
                pass

    def get_daily_ohlcv(self, symbol: str, count: int = 100):
        df = self._fetch_from_163(symbol, count)
        if df is not None:
            return df
        df = self._fetch_from_baostock(symbol, count)
        if df is not None:
            return df
        return self._generate_fake_ohlcv(symbol, count)

    def _fetch_from_163(self, symbol: str, count: int):
        try:
            if symbol.startswith('sh.'):
                code_163 = f"0#{symbol[3:]}"
            elif symbol.startswith('sz.'):
                code_163 = f"1#{symbol[3:]}"
            else:
                code_163 = f"0#{symbol}"
            url = f"http://quotes.money.163.com/service/chddata.html?code={code_163}&start=19900101"
            df = pd.read_csv(url, encoding='gbk')
            if df.empty:
                return None
            df.rename(columns={'日期':'date','开盘价':'open','最高价':'high','最低价':'low','收盘价':'close','成交量':'volume'}, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            return df[['date','open','high','low','close','volume']].iloc[-count:]
        except:
            return None

    def _fetch_from_baostock(self, symbol: str, count: int):
        if not self._baostock_logged:
            return None
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=count*2)).strftime('%Y-%m-%d')
        try:
            rs = bs.query_history_k_data_plus(symbol, "date,open,high,low,close,volume", start, end, "d", "2")
            if rs.error_code != '0':
                return None
            data = []
            while rs.next():
                data.append(rs.get_row_data())
            if not data:
                return None
            df = pd.DataFrame(data, columns=['date','open','high','low','close','volume'])
            for c in ['open','high','low','close','volume']:
                df[c] = pd.to_numeric(df[c])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            return df[['date','open','high','low','close','volume']].iloc[-count:]
        except:
            return None

    def _generate_fake_ohlcv(self, symbol: str, count: int):
        dates = pd.date_range(end=datetime.today(), periods=count, freq='D')
        close = np.cumsum(np.random.randn(count)) + 1000
        open_ = close + np.random.randn(count) * 2
        high = close + abs(np.random.randn(count)) * 3
        low = close - abs(np.random.randn(count)) * 3
        volume = np.random.randint(100000, 1000000, count)
        df = pd.DataFrame({'date':dates, 'open':open_, 'high':high, 'low':low, 'close':close, 'volume':volume})
        logger.warning(f"使用模拟数据 for {symbol}，请勿用于实盘")
        return df

    def get_valuation(self, sector: str) -> Optional[Dict]:
        csv_path = "data/valuation.csv"
        if not os.path.exists(csv_path):
            logger.warning(f"估值文件 {csv_path} 不存在")
            return None
        try:
            df = pd.read_csv(csv_path)
            sector_df = df[df['sector'] == sector].sort_values('date', ascending=False)
            if sector_df.empty:
                return None
            latest = sector_df.iloc[0]
            return {'pe_percentile': float(latest['pe_percentile']), 'pb_percentile': float(latest['pb_percentile'])}
        except Exception as e:
            logger.error(f"读取估值失败: {e}")
            return None
