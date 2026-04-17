import time
import random
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseCrawler:
    def __init__(self, proxy: Optional[str] = None):
        self.proxy = {'http': proxy, 'https': proxy} if proxy else None
        self.max_retries = 2
        self.retry_delay = [1, 2]

    def fetch(self, url, timeout=15):
        for i in range(self.max_retries):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = requests.get(url, headers=headers, proxies=self.proxy, timeout=timeout)
                resp.encoding = 'utf-8'
                if resp.status_code == 200:
                    return resp
            except Exception as e:
                logger.warning(f"请求失败 {url}: {e}")
            time.sleep(self.retry_delay[i] + random.uniform(0.5, 1))
        return None

class NewsCrawler(BaseCrawler):
    def fetch_rss_feed(self, url, max_news=20) -> List[Dict]:
        """通用RSS解析"""
        resp = self.fetch(url)
        if not resp:
            return []
        try:
            root = ET.fromstring(resp.content)
            items = root.findall('.//item') or root.findall('.//entry')
            news_list = []
            for item in items[:max_news]:
                title = item.find('title').text if item.find('title') is not None else ''
                link = item.find('link').text if item.find('link') is not None else ''
                pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ''
                if not title:
                    continue
                news_list.append({
                    'title': title.strip(),
                    'time': pub_date,
                    'source': 'RSS',
                    'summary': title.strip(),
                    'url': link
                })
            return news_list
        except Exception as e:
            logger.error(f"RSS解析失败: {e}")
            return []

    def fetch_sina_rss(self, max_news=20) -> List[Dict]:
        """新浪财经RSS（较稳定）"""
        url = "http://feed.sina.com.cn/news/roll/roll_news.xml"
        return self.fetch_rss_feed(url, max_news)

    def fetch_163_rss(self, max_news=20) -> List[Dict]:
        """网易财经RSS备用"""
        url = "https://news.163.com/special/00011K6L/rss_news.xml"
        return self.fetch_rss_feed(url, max_news)

    def fetch_all_sources(self, max_news=30) -> List[Dict]:
        all_news = self.fetch_sina_rss(max_news)
        if len(all_news) < max_news:
            all_news += self.fetch_163_rss(max_news - len(all_news))
        # 去重
        seen = set()
        unique = []
        for n in all_news:
            if n['title'] not in seen:
                seen.add(n['title'])
                unique.append(n)
        return unique[:max_news]

class FundDataCrawler(BaseCrawler):
    def __init__(self, proxy=None):
        super().__init__(proxy)
        # 尝试导入 baostock，如果没有则安装
        try:
            import baostock as bs
            self.bs = bs
            self._logged_in = False
        except ImportError:
            logger.warning("baostock未安装，将使用模拟数据")
            self.bs = None

    def _ensure_login(self):
        if self.bs and not self._logged_in:
            try:
                lg = self.bs.login()
                if lg.error_code == '0':
                    self._logged_in = True
            except:
                pass

    def fetch_north_flow(self) -> Dict:
        """北向资金 - 返回模拟数据（因免费接口不稳定）"""
        # 实际中可用 akshare 但经常变，这里返回空，不影响主流程
        return {}

    def fetch_sector_flow(self) -> List[Dict]:
        """板块资金流 - 返回空列表"""
        return []

    def fetch_market_data(self) -> Dict:
        """使用 baostock 获取上证指数日线"""
        if not self.bs:
            return self._fetch_market_akshare()
        self._ensure_login()
        try:
            # 获取最新一天数据
            rs = self.bs.query_history_k_data_plus("sh.000001",
                "date,open,high,low,close,volume",
                start_date=(datetime.now().strftime('%Y-%m-%d')),
                end_date=(datetime.now().strftime('%Y-%m-%d')),
                frequency="d", adjustflag="3")
            if rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
                return {
                    'date': row[0],
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': int(row[5])
                }
        except Exception as e:
            logger.error(f"baostock获取大盘失败: {e}")
        return self._fetch_market_akshare()

    def _fetch_market_akshare(self) -> Dict:
        """备选：akshare获取大盘"""
        try:
            import akshare as ak
            df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="", end_date="", adjust="qfq")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {
                    'date': latest['日期'],
                    'open': latest['开盘'],
                    'high': latest['最高'],
                    'low': latest['最低'],
                    'close': latest['收盘'],
                    'volume': latest['成交量']
                }
        except Exception as e:
            logger.error(f"akshare大盘失败: {e}")
        return {}

    def fetch_all(self) -> Dict:
        return {'north_flow': self.fetch_north_flow(), 'sector_flows': self.fetch_sector_flow()}
