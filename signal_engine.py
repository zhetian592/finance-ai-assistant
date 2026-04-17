# -*- coding: utf-8 -*-
"""
信号合成模块：过滤条件 + 仓位计算
"""

import logging
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class SignalEngine:
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self.min_score = config.get('min_score', 8)
        self.min_confidence = config.get('min_confidence', 0.7)
        self.allow_expectations = config.get('allow_expectations', ['超预期', '部分预期'])
    
    def generate_signals(self):
        """生成信号列表"""
        news_list = self.db.get_latest_news(limit=30)
        market_data = self.db.get_latest_market()
        fund_flows = self.db.get_recent_fund_flow(days=3)
        
        signals = []
        for news in news_list:
            # 检查是否已分析
            if news['sentiment'] is None:
                continue
            # 基础过滤
            if news['score'] < self.min_score:
                continue
            if news['confidence'] < self.min_confidence:
                continue
            if news['expectation'] not in self.allow_expectations:
                continue
            
            # 大盘环境检查（均线多头）
            market_ok = self.check_market_trend(market_data)
            if not market_ok:
                logger.info("大盘环境不符合多头排列，跳过信号")
                continue
            
            # 涨跌家数比（简化：从外部获取，这里假设为0.8）
            sentiment_ratio = self.get_sentiment_ratio()
            if sentiment_ratio < 0.7:
                logger.info(f"涨跌家数比 {sentiment_ratio} < 0.7，跳过")
                continue
            
            # 板块资金流检查
            sectors = news['beneficial_sectors'].split(',') if news['beneficial_sectors'] else []
            sector_flow_ok = self.check_sector_flow(sectors, fund_flows)
            if not sector_flow_ok:
                logger.info(f"板块资金流不满足条件，跳过")
                continue
            
            # 计算仓位
            position = self.calculate_position(news, market_ok, sector_flow_ok, sentiment_ratio)
            
            signals.append({
                'news_id': news['id'],
                'title': news['title'],
                'sentiment': news['sentiment'],
                'score': news['score'],
                'confidence': news['confidence'],
                'expectation': news['expectation'],
                'sectors': sectors,
                'position': position,
                'action': 'buy' if position > 0 else 'wait'
            })
        return signals
    
    def check_market_trend(self, market_data):
        """检查均线多头排列（至少3根日线）"""
        if len(market_data) < 20:
            return True  # 数据不足时默认通过
        closes = [row['close'] for row in market_data[:20]]
        if len(closes) < 20:
            return True
        ma20 = np.mean(closes)
        ma60 = np.mean(closes[:60]) if len(closes) >= 60 else ma20
        ma120 = np.mean(closes[:120]) if len(closes) >= 120 else ma20
        # 多头排列：ma20 > ma60 > ma120
        return ma20 > ma60 > ma120
    
    def get_sentiment_ratio(self):
        """获取全市场涨跌家数比（简化：返回默认值0.8）"""
        # 实际可通过 AKShare 获取，此处简化
        return 0.8
    
    def check_sector_flow(self, sectors, fund_flows):
        """板块资金净流入检查"""
        if not sectors or sectors == ['待核实']:
            return True  # 无明确板块时默认通过
        # 简化：只要有一个板块有净流入即通过
        inflow_sectors = set()
        for flow in fund_flows:
            if flow['type'] == 'sector' and flow['value'] > 0:
                inflow_sectors.add(flow['sector'])
        for sec in sectors:
            if sec in inflow_sectors:
                return True
        return False
    
    def calculate_position(self, news, market_ok, sector_flow_ok, sentiment_ratio):
        """仓位计算 (score/10) * confidence * market_factor * flow_factor * sentiment_factor * 0.05"""
        score_norm = news['score'] / 10.0
        confidence = news['confidence']
        market_factor = 1.0 if market_ok else 0.5
        flow_factor = 1.0 if sector_flow_ok else 0.5
        # sentiment_factor 根据涨跌家数比映射
        if sentiment_ratio >= 0.9:
            sentiment_factor = 1.0
        elif sentiment_ratio >= 0.7:
            sentiment_factor = 0.8
        else:
            sentiment_factor = 0.5
        total = score_norm * confidence * market_factor * flow_factor * sentiment_factor * 0.05
        return min(total, 0.05)  # 单笔最大5%
