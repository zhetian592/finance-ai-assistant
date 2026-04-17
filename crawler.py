# -*- coding: utf-8 -*-
"""
爬虫模块：新闻、资金数据、大盘数据
"""

import time
import random
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from typing import List, Dict, Optional
import logging
import akshare as ak

logger = logging.getLogger(__name__)

class BaseCrawler:
    def __init__(self, proxy: Optional[str] = None):
        self.proxy = {'http': proxy, 'https': proxy} if proxy else None
        self.ua = UserAgent()
        self.max_retries = 3
        self.retry_delay = [1, 2, 4]  # 指数退避
    
    def get_headers(self):
        return {'User-Agent': self.ua.random}
    
    def fetch(self, url, timeout=15):
        for i in range(self.max_retries):
            try:
                resp = requests.get(
                    url, 
                    headers=self.get_headers(), 
                    proxies=self.proxy,
                    timeout=timeout
                )
                resp.encoding = 'utf-8'
                if resp.status_code == 200:
                    return resp
                else:
                    logger.warning(f"请求失败 {url} 状态码 {resp.status_code}, 重试 {i+1}/{self.max_retries}")
            except Exception as e:
                logger.warning(f"请求异常 {url}: {e}, 重试 {i+1}/{self.max_retries}")
            time.sleep(self.retry_delay[i] + random.uniform(0.5, 1.5))
        return None

class NewsCrawler(BaseCrawler):
    """新闻爬虫：新浪财经、财联社快讯"""
    
    def fetch_sina_roll(self, max_news=20) -> List[Dict]:
        """新浪财经滚动新闻"""
        url = "https://finance.sina.com.cn/roll/"
        resp = self.fetch(url)
        if not resp:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        news_list = []
        items = soup.select('.news-item') or soup.select('.list li') or soup.select('.d-list li')
        
        for item in items[:max_news]:
            try:
                a_tag = item.select_one('a')
                if not a_tag:
                    continue
                title = a_tag.get_text(strip=True)
                link = a_tag.get('href', '')
                if not title or not link:
                    continue
                # 时间、来源处理
                time_tag = item.select_one('.time') or item.select_one('span.time')
                time_str = time_tag.get_text(strip=True) if time_tag else ""
                source_tag = item.select_one('.source') or item.select_one('span.source')
                source = source_tag.get_text(strip=True) if source_tag else "新浪财经"
                summary = title  # 简化
                news_list.append({
                    'title': title,
                    'time': time_str,
                    'source': source,
                    'summary': summary,
                    'url': link
                })
            except Exception as e:
                logger.error(f"解析新浪新闻条目失败: {e}")
        return news_list
    
    def fetch_cls_express(self, max_news=20) -> List[Dict]:
        """财联社快讯"""
        url = "https://www.cls.cn/telegraph"
        resp = self.fetch(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        news_list = []
        # 财联社快讯结构通常为 .telegraph-list .telegraph-item
        items = soup.select('.telegraph-item') or soup.select('.list-item')
        for item in items[:max_news]:
            try:
                title_elem = item.select_one('.title') or item.select_one('a')
                title = title_elem.get_text(strip=True) if title_elem else ""
                link = title_elem.get('href') if title_elem else ""
                if link and not link.startswith('http'):
                    link = "https://www.cls.cn" + link
                time_elem = item.select_one('.time')
                time_str = time_elem.get_text(strip=True) if time_elem else ""
                news_list.append({
                    'title': title,
                    'time': time_str,
                    'source': '财联社',
                    'summary': title,
                    'url': link
                })
            except Exception as e:
                logger.error(f"解析财联社新闻失败: {e}")
        return news_list
    
    def fetch_all_sources(self, max_news=30) -> List[Dict]:
        """多源获取，去重（按标题）"""
        all_news = []
        # 尝试新浪
        sina_news = self.fetch_sina_roll(max_news)
        all_news.extend(sina_news)
        # 尝试财联社
        if len(all_news) < max_news:
            cls_news = self.fetch_cls_express(max_news - len(all_news))
            all_news.extend(cls_news)
        # 简单去重（按标题）
        seen_titles = set()
        unique_news = []
        for news in all_news:
            title = news['title']
            if title not in seen_titles:
                seen_titles.add(title)
                unique_news.append(news)
        # 随机延时
        time.sleep(random.uniform(1, 3))
        return unique_news[:max_news]


class FundDataCrawler(BaseCrawler):
    """资金数据爬虫：使用 AKShare"""
    
    def fetch_north_flow(self) -> Dict:
        """北向资金估算（当日）"""
        try:
            # AKShare 接口：获取北向资金当日流向
            df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {
                    'date': latest['日期'],
                    'net_inflow': latest['净买入额'],
                    'type': 'north'
                }
        except Exception as e:
            logger.error(f"获取北向资金失败: {e}")
        return {}
    
    def fetch_sector_flow(self) -> List[Dict]:
        """板块资金净流入（申万一级）"""
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流向")
            if df is not None and not df.empty:
                result = []
                for _, row in df.head(10).iterrows():
                    result.append({
                        'sector': row['行业'],
                        'net_inflow': row['今日净流入'],
                        'date': datetime.now().strftime('%Y-%m-%d')
                    })
                return result
        except Exception as e:
            logger.error(f"获取板块资金流失败: {e}")
        return []
    
    def fetch_market_data(self) -> Dict:
        """上证指数日线"""
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
            logger.error(f"获取上证指数失败: {e}")
        return {}
    
    def fetch_all(self) -> Dict:
        """整合所有资金数据"""
        north = self.fetch_north_flow()
        sectors = self.fetch_sector_flow()
        return {
            'north_flow': north,
            'sector_flows': sectors
        }
