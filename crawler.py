import time
import random
import requests
import re
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from typing import List, Dict, Optional
import logging
import akshare as ak
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseCrawler:
    def __init__(self, proxy: Optional[str] = None):
        self.proxy = {'http': proxy, 'https': proxy} if proxy else None
        self.ua = UserAgent()
        self.max_retries = 3
        self.retry_delay = [1, 2, 4]

    def get_headers(self):
        return {'User-Agent': self.ua.random}

    def fetch(self, url, timeout=15):
        for i in range(self.max_retries):
            try:
                resp = requests.get(url, headers=self.get_headers(), proxies=self.proxy, timeout=timeout)
                resp.encoding = 'utf-8'
                if resp.status_code == 200:
                    return resp
            except Exception as e:
                logger.warning(f"请求异常 {url}: {e}, 重试 {i+1}")
            time.sleep(self.retry_delay[i] + random.uniform(0.5, 1.5))
        return None

class NewsCrawler(BaseCrawler):
    def fetch_sina_roll(self, max_news=20) -> List[Dict]:
        url = "https://finance.sina.com.cn/roll/"
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        news_list = []
        # 所有可能包含新闻的链接
        links = soup.find_all('a', href=re.compile(r'https://finance\.sina\.com\.cn/.*/\d+/\d+\.shtml'))
        seen = set()
        for a in links[:max_news*2]:
            href = a.get('href')
            if href in seen:
                continue
            seen.add(href)
            title = a.get_text(strip=True)
            if not title or len(title) < 5:
                continue
            news_list.append({
                'title': title,
                'time': datetime.now().strftime('%Y-%m-%d %H:%M'),
                'source': '新浪财经',
                'summary': title,
                'url': href
            })
            if len(news_list) >= max_news:
                break
        return news_list

    def fetch_cls_express(self, max_news=20) -> List[Dict]:
        url = "https://www.cls.cn/telegraph"
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        news_list = []
        items = soup.select('.telegraph-item') or soup.select('.list-item')
        for item in items[:max_news]:
            try:
                a_tag = item.select_one('a')
                if not a_tag:
                    continue
                title = a_tag.get_text(strip=True)
                href = a_tag.get('href', '')
                if href and not href.startswith('http'):
                    href = 'https://www.cls.cn' + href
                if not title or len(title) < 5:
                    continue
                time_elem = item.select_one('.time')
                time_str = time_elem.get_text(strip=True) if time_elem else ""
                news_list.append({
                    'title': title,
                    'time': time_str,
                    'source': '财联社',
                    'summary': title,
                    'url': href
                })
            except:
                pass
        return news_list

    def fetch_all_sources(self, max_news=30) -> List[Dict]:
        all_news = self.fetch_sina_roll(max_news)
        if len(all_news) < max_news:
            all_news += self.fetch_cls_express(max_news - len(all_news))
        # 去重
        seen = set()
        unique = []
        for n in all_news:
            if n['title'] not in seen:
                seen.add(n['title'])
                unique.append(n)
        return unique[:max_news]

class FundDataCrawler(BaseCrawler):
    def fetch_north_flow(self) -> Dict:
        try:
            df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {'date': latest['日期'], 'net_inflow': latest['净买入额'], 'type': 'north'}
        except Exception as e:
            logger.error(f"北向资金失败: {e}")
        return {}

    def fetch_sector_flow(self) -> List[Dict]:
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流向")
            if df is None or df.empty:
                return []
            # 动态获取列名
            sector_col = '行业' if '行业' in df.columns else '名称'
            inflow_col = '今日净流入' if '今日净流入' in df.columns else '净流入额'
            result = []
            for _, row in df.head(10).iterrows():
                result.append({
                    'sector': row[sector_col],
                    'net_inflow': row[inflow_col],
                    'date': datetime.now().strftime('%Y-%m-%d')
                })
            return result
        except Exception as e:
            logger.error(f"板块资金流失败: {e}")
            return []

    def fetch_market_data(self) -> Dict:
        try:
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
            logger.error(f"大盘数据失败: {e}")
        return {}

    def fetch_all(self) -> Dict:
        return {'north_flow': self.fetch_north_flow(), 'sector_flows': self.fetch_sector_flow()}
