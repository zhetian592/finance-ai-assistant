# -*- coding: utf-8 -*-
"""
爬虫模块：新闻、资金数据、大盘数据（修复版）
"""

import time
import random
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from typing import List, Dict, Optional
import logging
import akshare as ak
import re

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
    def fetch_sina_roll(self, max_news=20) -> List[Dict]:
        """新浪财经滚动新闻 - 通用选择器"""
        url = "https://finance.sina.com.cn/roll/"
        resp = self.fetch(url)
        if not resp:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        news_list = []
        
        # 多种选择器尝试
        selectors = [
            '.news-item', '.list li', '.d-list li', 
            '.feed-card-item', '.feed-card-content', 
            'a[href*="finance.sina.com.cn"]'
        ]
        items = []
        for sel in selectors:
            items = soup.select(sel)
            if items:
                break
        
        # 如果还是没有，直接找所有包含新闻链接的 a 标签
        if not items:
            all_links = soup.find_all('a', href=re.compile(r'https://finance\.sina\.com\.cn/.*/.*\.shtml'))
            items = all_links[:max_news*2]
        
        seen_urls = set()
        for item in items:
            try:
                a_tag = item if item.name == 'a' else item.select_one('a')
                if not a_tag:
                    continue
                href = a_tag.get('href', '')
                if not href or not href.startswith('https://finance.sina.com.cn/'):
                    continue
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 5:
                    continue
                
                # 时间尝试从附近 span 获取
                time_elem = item.select_one('.time') or item.select_one('.date') or item.select_one('span.time')
                time_str = time_elem.get_text(strip=True) if time_elem else ""
                source = "新浪财经"
                summary = title  # 简化
                
                news_list.append({
                    'title': title,
                    'time': time_str,
                    'source': source,
                    'summary': summary,
                    'url': href
                })
                if len(news_list) >= max_news:
                    break
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
        
        # 常见选择器
        items = soup.select('.telegraph-item') or soup.select('.list-item') or soup.select('.news-item')
        if not items:
            # 尝试找所有带标题的链接
            items = soup.find_all('a', href=re.compile(r'/detail/\d+'))
        
        for item in items[:max_news]:
            try:
                a_tag = item if item.name == 'a' else item.select_one('a')
                if not a_tag:
                    continue
                title = a_tag.get_text(strip=True)
                href = a_tag.get('href', '')
                if href and not href.startswith('http'):
                    href = 'https://www.cls.cn' + href
                if not title or len(title) < 5:
                    continue
                time_elem = item.select_one('.time') or item.select_one('.date')
                time_str = time_elem.get_text(strip=True) if time_elem else ""
                news_list.append({
                    'title': title,
                    'time': time_str,
                    'source': '财联社',
                    'summary': title,
                    'url': href
                })
            except Exception as e:
                logger.error(f"解析财联社新闻失败: {e}")
        return news_list
    
    def fetch_all_sources(self, max_news=30) -> List[Dict]:
        all_news = []
        sina_news = self.fetch_sina_roll(max_news)
        all_news.extend(sina_news)
        if len(all_news) < max_news:
            cls_news = self.fetch_cls_express(max_news - len(all_news))
            all_news.extend(cls_news)
        
        # 去重（按标题）
        seen_titles = set()
        unique_news = []
        for news in all_news:
            title = news['title']
            if title not in seen_titles:
                seen_titles.add(title)
                unique_news.append(news)
        time.sleep(random.uniform(1, 2))
        return unique_news[:max_news]


class FundDataCrawler(BaseCrawler):
    """资金数据爬虫 - 使用 AKShare 最新接口"""
    
    def fetch_north_flow(self) -> Dict:
        """北向资金估算（当日）"""
        try:
            # 修正接口：stock_hsgt_north_net_flow_in_em 可能已变更，改用 stock_hsgt_north_net_flow_in_em
            # 先尝试新接口
            df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {
                    'date': latest['日期'],
                    'net_inflow': latest['净买入额'],
                    'type': 'north'
                }
        except AttributeError:
            # 旧接口备选
            try:
                df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    return {
                        'date': latest['日期'],
                        'net_inflow': latest['净买入额'],
                        'type': 'north'
                    }
            except:
                pass
        except Exception as e:
            logger.error(f"获取北向资金失败: {e}")
        return {}
    
    def fetch_sector_flow(self) -> List[Dict]:
        """板块资金净流入（申万一级）"""
        try:
            # 修正接口参数
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流向")
            if df is not None and not df.empty:
                result = []
                for _, row in df.head(10).iterrows():
                    # 列名可能是 '行业' 或 '名称'
                    sector_col = '行业' if '行业' in df.columns else '名称'
                    inflow_col = '今日净流入' if '今日净流入' in df.columns else '净流入'
                    result.append({
                        'sector': row[sector_col],
                        'net_inflow': row[inflow_col],
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
        north = self.fetch_north_flow()
        sectors = self.fetch_sector_flow()
        return {
            'north_flow': north,
            'sector_flows': sectors
        }
