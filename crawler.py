import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List, Dict
import time
import random

logger = logging.getLogger(__name__)

class NewsCrawler:
    """多源新闻爬虫（HTML解析版）"""
    
    def fetch_sina_roll(self, max_news=30) -> List[Dict]:
        """新浪财经滚动新闻 - HTML 解析"""
        url = "https://finance.sina.com.cn/roll/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = 'utf-8'
            if resp.status_code != 200:
                logger.warning(f"新浪滚动页状态码 {resp.status_code}")
                return []
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            news_list = []
            
            # 查找所有符合财经新闻链接的 a 标签
            all_links = soup.find_all('a', href=True)
            for a in all_links:
                href = a['href']
                title = a.get_text(strip=True)
                # 过滤条件：链接包含 finance.sina.com.cn，标题长度合适
                if ('finance.sina.com.cn' in href and 
                    len(title) > 8 and 
                    '滚动' not in title and
                    '图集' not in title):
                    # 避免重复
                    if any(n['url'] == href for n in news_list):
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
            logger.info(f"新浪爬取 {len(news_list)} 条")
            return news_list
        except Exception as e:
            logger.error(f"新浪爬取失败: {e}")
            return []

    def fetch_tencent_news(self, max_news=20) -> List[Dict]:
        """腾讯财经新闻（备用源）"""
        url = "https://finance.qq.com/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = 'utf-8'
            if resp.status_code != 200:
                return []
            soup = BeautifulSoup(resp.text, 'html.parser')
            news_list = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                title = a.get_text(strip=True)
                if ('finance.qq.com' in href and 
                    len(title) > 8 and 
                    '直播' not in title and
                    '视频' not in title):
                    if any(n['url'] == href for n in news_list):
                        continue
                    news_list.append({
                        'title': title,
                        'time': datetime.now().strftime('%Y-%m-%d %H:%M'),
                        'source': '腾讯财经',
                        'summary': title,
                        'url': href
                    })
                    if len(news_list) >= max_news:
                        break
            logger.info(f"腾讯爬取 {len(news_list)} 条")
            return news_list
        except Exception as e:
            logger.error(f"腾讯爬取失败: {e}")
            return []

    def fetch_163_news(self, max_news=20) -> List[Dict]:
        """网易财经新闻（备用源）"""
        url = "https://money.163.com/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = 'utf-8'
            if resp.status_code != 200:
                return []
            soup = BeautifulSoup(resp.text, 'html.parser')
            news_list = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                title = a.get_text(strip=True)
                if ('money.163.com' in href and 
                    len(title) > 8 and 
                    '专题' not in title):
                    if any(n['url'] == href for n in news_list):
                        continue
                    news_list.append({
                        'title': title,
                        'time': datetime.now().strftime('%Y-%m-%d %H:%M'),
                        'source': '网易财经',
                        'summary': title,
                        'url': href
                    })
                    if len(news_list) >= max_news:
                        break
            logger.info(f"网易爬取 {len(news_list)} 条")
            return news_list
        except Exception as e:
            logger.error(f"网易爬取失败: {e}")
            return []

    def fetch_all_sources(self, max_news=30) -> List[Dict]:
        """多源获取，优先级：新浪 > 腾讯 > 网易"""
        all_news = []
        
        # 新浪
        sina_news = self.fetch_sina_roll(max_news)
        all_news.extend(sina_news)
        
        # 如果新浪不足，补充腾讯
        if len(all_news) < max_news:
            tencent_news = self.fetch_tencent_news(max_news - len(all_news))
            all_news.extend(tencent_news)
        
        # 如果还不够，补充网易
        if len(all_news) < max_news:
            netease_news = self.fetch_163_news(max_news - len(all_news))
            all_news.extend(netease_news)
        
        # 去重（按标题）
        seen = set()
        unique = []
        for n in all_news:
            if n['title'] not in seen:
                seen.add(n['title'])
                unique.append(n)
        
        logger.info(f"总计获取 {len(unique)} 条新闻")
        return unique[:max_news]


class FundDataCrawler:
    """资金数据（降级处理，失败不影响主流程）"""
    
    def fetch_north_flow(self) -> Dict:
        try:
            import akshare as ak
            # 新版本 akshare 接口名可能有变化，尝试多个
            try:
                df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            except AttributeError:
                df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {'date': latest['日期'], 'net_inflow': latest['净买入额']}
        except Exception as e:
            logger.warning(f"北向资金获取失败: {e}")
        return {}

    def fetch_sector_flow(self) -> List[Dict]:
        try:
            import akshare as ak
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流向")
            if df is None or df.empty:
                return []
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
            logger.warning(f"板块资金获取失败: {e}")
        return []

    def fetch_market_data(self) -> Dict:
        try:
            import akshare as ak
            df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="", end_date="", adjust="qfq")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {
                    'date': latest['日期'],
                    'close': latest['收盘'],
                    'volume': latest['成交量']
                }
        except Exception as e:
            logger.warning(f"大盘数据获取失败: {e}")
        return {}

    def fetch_all(self) -> Dict:
        return {
            'north_flow': self.fetch_north_flow(),
            'sector_flows': self.fetch_sector_flow()
        }
