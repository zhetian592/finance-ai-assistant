# data_services.py
import pandas as pd
import numpy as np
import requests
import baostock as bs
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
import os
import time

logger = logging.getLogger(__name__)

class DataService:
    """统一数据服务：提供行业指数行情和估值数据"""
    
    def __init__(self, cache_dir=".cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._baostock_logged = False
        self._init_baostock()
    
    def _init_baostock(self):
        """Baostock 登录（懒加载）"""
        if not self._baostock_logged:
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    self._baostock_logged = True
                    logger.info("Baostock login success")
                else:
                    logger.error(f"Baostock login fail: {lg.error_msg}")
            except Exception as e:
                logger.error(f"Baostock login exception: {e}")
    
    # ================= 行情数据（日K线）=================
    def get_daily_ohlcv(self, symbol: str, count: int = 100) -> Optional[pd.DataFrame]:
        """
        获取行业指数日K线（open, high, low, close, volume）
        降级顺序：163.com → baostock → 模拟数据
        symbol: 例如 "sh.000300" 或 "sz.399300"
        """
        # 1. 尝试 163 数据源（免费、稳定、无需注册）
        df = self._fetch_from_163(symbol, count)
        if df is not None:
            return df
        logger.warning(f"{symbol} 163源失败，尝试 baostock")
        
        # 2. 尝试 baostock
        df = self._fetch_from_baostock(symbol, count)
        if df is not None:
            return df
        logger.warning(f"{symbol} baostock源失败，使用模拟数据（仅供占位）")
        
        # 3. 最后的降级：生成模拟数据（避免程序崩溃）
        return self._generate_fake_ohlcv(symbol, count)
    
    def _fetch_from_163(self, symbol: str, count: int) -> Optional[pd.DataFrame]:
        """从 163.com 获取历史数据（CSV格式）"""
        try:
            # 转换代码格式：例如 sh.000300 → 0#000300, sz.399300 → 1#399300
            code_163 = self._convert_to_163_code(symbol)
            url = f"http://quotes.money.163.com/service/chddata.html?code={code_163}&start=19900101&end={datetime.now().strftime('%Y%m%d')}"
            df = pd.read_csv(url, encoding='gbk')
            if df.empty:
                return None
            
            # 标准化列名
            df.rename(columns={
                '日期': 'date',
                '开盘价': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '收盘价': 'close',
                '成交量': 'volume'
            }, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            # 取最近 count 条
            df = df.iloc[-count:]
            return df[['open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            logger.debug(f"163 fetch failed for {symbol}: {e}")
            return None
    
    def _convert_to_163_code(self, symbol: str) -> str:
        """将内部代码转为163格式：sh.000300 -> 0#000300, sz.399300 -> 1#399300"""
        if symbol.startswith('sh.'):
            return f"0#{symbol[3:]}"
        elif symbol.startswith('sz.'):
            return f"1#{symbol[3:]}"
        else:
            # 默认当作上海
            return f"0#{symbol}"
    
    def _fetch_from_baostock(self, symbol: str, count: int) -> Optional[pd.DataFrame]:
        """从 baostock 获取日K线"""
        if not self._baostock_logged:
            self._init_baostock()
            if not self._baostock_logged:
                return None
        
        # baostock 代码直接使用传入的 symbol（如 "sh.000300"）
        start_date = (datetime.now() - timedelta(days=count*2)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        try:
            rs = bs.query_history_k_data_plus(
                symbol,
                "date,open,high,low,close,volume",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"  # 前复权
            )
            if rs.error_code != '0':
                return None
            data = []
            while rs.next():
                data.append(rs.get_row_data())
            if not data:
                return None
            df = pd.DataFrame(data, columns=['date','open','high','low','close','volume'])
            for col in ['open','high','low','close','volume']:
                df[col] = pd.to_numeric(df[col])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            return df[['open','high','low','close','volume']].iloc[-count:]
        except Exception as e:
            logger.debug(f"Baostock fetch failed for {symbol}: {e}")
            return None
    
    def _generate_fake_ohlcv(self, symbol: str, count: int) -> pd.DataFrame:
        """仅用于占位，返回模拟数据（避免程序崩溃）"""
        dates = pd.date_range(end=datetime.today(), periods=count, freq='D')
        close = np.cumsum(np.random.randn(count)) + 1000
        open_ = close + np.random.randn(count) * 2
        high = close + abs(np.random.randn(count)) * 3
        low = close - abs(np.random.randn(count)) * 3
        volume = np.random.randint(100000, 1000000, count)
        df = pd.DataFrame({
            'date': dates,
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })
        logger.warning(f"使用模拟数据 for {symbol}, 真实交易请勿依赖")
        return df
    
    # ================= 估值数据（PE/PB 历史分位）=================
    def get_valuation(self, sector_name: str) -> Optional[dict]:
        """
        从本地 CSV 文件读取行业估值分位数据
        用户需要手动维护 data/valuation.csv，格式见说明
        """
        csv_path = "data/valuation.csv"
        if not os.path.exists(csv_path):
            logger.warning(f"估值文件 {csv_path} 不存在，请手动创建并录入数据")
            return None
        
        try:
            df = pd.read_csv(csv_path)
            # 过滤对应行业最新一条记录
            sector_df = df[df['sector'] == sector_name].sort_values('date', ascending=False)
            if sector_df.empty:
                return None
            latest = sector_df.iloc[0]
            return {
                'pe_percentile': float(latest['pe_percentile']),
                'pb_percentile': float(latest['pb_percentile']),
                'date': latest['date']
            }
        except Exception as e:
            logger.error(f"读取估值文件失败: {e}")
            return None

    # 可选：提供一个工具函数，方便手动更新估值数据到 CSV
    def save_valuation_manual(sector_name, pe_percentile, pb_percentile):
        """手动添加估值数据（可在外部调用）"""
        import csv
        os.makedirs("data", exist_ok=True)
        filepath = "data/valuation.csv"
        with open(filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if os.path.getsize(filepath) == 0:
                writer.writerow(['date', 'sector', 'pe_percentile', 'pb_percentile'])
            writer.writerow([datetime.now().strftime('%Y-%m-%d'), sector_name, pe_percentile, pb_percentile])
