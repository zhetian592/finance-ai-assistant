import requests
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
import time
import random

logger = logging.getLogger(__name__)

class NewsCrawler:
    """稳定版：使用新浪 JSON 接口 + 网易备用"""
    
    def fetch_sina_json(self, max_news=30) -> List[Dict]:
        url = "https://feed.sina.com.cn/news/roll/roll_info_1.xml?show=title&format=json"
        try:
            resp = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code != 200:
                return []
            data = resp.json()
            items = data.get('result', {}).get('data', [])
            news = []
            for item in items[:max_news]:
                title = item.get('title', '').strip()
                link = item.get('url', '')
                time_str = item.get('ctime', '')
                if title and link:
                    news.append({
                        'title': title,
                        'time': time_str,
                        'source': '新浪财经',
                        'summary': title,
                        'url': link
                    })
            logger.info(f"新浪获取 {len(news)} 条")
            return news
        except Exception as e:
            logger.error(f"新浪接口失败: {e}")
            return []

    def fetch_163_json(self, max_news=20) -> List[Dict]:
        """网易财经 JSON 接口（备用）"""
        url = "https://c.m.163.com/news/headline/T1467284926140.json"
        try:
            resp = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code != 200:
                return []
            data = resp.json()
            items = data.get('data', [])
            news = []
            for item in items[:max_news]:
                title = item.get('title', '').strip()
                link = item.get('url', '')
                time_str = datetime.fromtimestamp(item.get('ctime', 0)).strftime('%Y-%m-%d %H:%M')
                if title and link:
                    news.append({
                        'title': title,
                        'time': time_str,
                        'source': '网易财经',
                        'summary': title,
                        'url': link
                    })
            logger.info(f"网易获取 {len(news)} 条")
            return news
        except Exception as e:
            logger.error(f"网易接口失败: {e}")
            return []

    def fetch_all_sources(self, max_news=30) -> List[Dict]:
        news = self.fetch_sina_json(max_news)
        if len(news) < max_news:
            news += self.fetch_163_json(max_news - len(news))
        # 去重
        seen = set()
        unique = []
        for n in news:
            if n['title'] not in seen:
                seen.add(n['title'])
                unique.append(n)
        return unique[:max_news]


class FundDataCrawler:
    """资金数据（降级处理：失败时返回空，不中断流程）"""
    
    def fetch_north_flow(self) -> Dict:
        try:
            import akshare as ak
            df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                return {'date': latest['日期'], 'net_inflow': latest['净买入额']}
        except Exception as e:
            logger.warning(f"北向资金获取失败（降级）: {e}")
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
            logger.warning(f"板块资金获取失败（降级）: {e}")
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
            logger.warning(f"大盘数据获取失败（降级）: {e}")
        return {}

    def fetch_all(self) -> Dict:
        return {
            'north_flow': self.fetch_north_flow(),
            'sector_flows': self.fetch_sector_flow()
        }
