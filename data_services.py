import pandas as pd
import numpy as np
import requests
import baostock as bs
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
import os
import re

logger = logging.getLogger(__name__)

class DataService:
    """统一数据服务 - 稳定版"""
    def __init__(self, cache_dir=".cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._baostock_logged = False
        self._init_baostock()
        
        # 简单情绪词库（无需额外模型）
        self._positive_words = ["上涨", "增长", "利好", "提振", "上升", "突破", "创新高", "看好", "买入", "乐观", "强劲", "超预期", "增持", "推荐"]
        self._negative_words = ["下跌", "下降", "利空", "打压", "下滑", "跌破", "亏损", "担忧", "卖出", "悲观", "疲软", "减持", "警示", "暴跌"]
    
    def _init_baostock(self):
        if not self._baostock_logged:
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    self._baostock_logged = True
                    logger.info("Baostock login success")
                else:
                    logger.error(f"Baostock login fail: {lg.error_msg}")
            except:
                pass
    
    # ==================== 行情数据（日K线）====================
    def get_daily_ohlcv(self, symbol: str, count: int = 100) -> Optional[pd.DataFrame]:
        """
        获取日线 OHLCV
        降级顺序：163.com → baostock → 模拟数据（防止崩溃）
        """
        df = self._fetch_from_163(symbol, count)
        if df is not None:
            return df
        logger.warning(f"{symbol} 163源失败，尝试 baostock")
        df = self._fetch_from_baostock(symbol, count)
        if df is not None:
            return df
        logger.warning(f"{symbol} 所有源失败，使用模拟数据")
        return self._generate_fake_ohlcv(symbol, count)
    
    def _fetch_from_163(self, symbol: str, count: int) -> Optional[pd.DataFrame]:
        try:
            # 转换代码: sh.000300 -> 0#000300, sz.399300 -> 1#399300
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
            df = df.iloc[-count:]
            return df[['date','open','high','low','close','volume']]
        except Exception as e:
            logger.debug(f"163 fetch error {symbol}: {e}")
            return None
    
    def _fetch_from_baostock(self, symbol: str, count: int) -> Optional[pd.DataFrame]:
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
        except Exception as e:
            logger.debug(f"Baostock error {symbol}: {e}")
            return None
    
    def _generate_fake_ohlcv(self, symbol: str, count: int) -> pd.DataFrame:
        dates = pd.date_range(end=datetime.today(), periods=count, freq='D')
        close = np.cumsum(np.random.randn(count)) + 1000
        open_ = close + np.random.randn(count) * 2
        high = close + abs(np.random.randn(count)) * 3
        low = close - abs(np.random.randn(count)) * 3
        volume = np.random.randint(100000, 1000000, count)
        df = pd.DataFrame({'date':dates, 'open':open_, 'high':high, 'low':low, 'close':close, 'volume':volume})
        logger.warning(f"使用模拟数据 for {symbol}，请勿用于实盘")
        return df
    
    # ==================== 估值数据（从本地 CSV 读取）====================
    def get_valuation(self, sector: str) -> Optional[Dict]:
        """返回 PE 分位 (0-100), 数值越低表示越低估"""
        csv_path = "data/valuation.csv"
        if not os.path.exists(csv_path):
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
            logger.error(f"读取估值失败: {e}")
            return None
    
    # ==================== 情绪分析（基于简单词库）====================
    def analyze_sentiment(self, text: str) -> Dict:
        """返回 {label: positive/negative/neutral, score: 0~1}"""
        pos_cnt = sum(1 for w in self._positive_words if w in text)
        neg_cnt = sum(1 for w in self._negative_words if w in text)
        if pos_cnt > neg_cnt:
            return {"label": "positive", "score": min(0.9, 0.5 + pos_cnt*0.1)}
        elif neg_cnt > pos_cnt:
            return {"label": "negative", "score": min(0.9, 0.5 + neg_cnt*0.1)}
        else:
            return {"label": "neutral", "score": 0.5}
