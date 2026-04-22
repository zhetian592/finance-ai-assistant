import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class FactorModel:
    """多因子模型，计算行业/基金的综合得分"""
    
    def __init__(self):
        # 因子权重（可调）
        self.weights = {
            "valuation": 0.2,    # 估值分位
            "growth": 0.3,       # 盈利增长
            "momentum": 0.2,     # 价格动量
            "sentiment": 0.15,   # 事件情感
            "liquidity": 0.15    # 资金流向
        }
        # 行业因子数据缓存
        self.cache = {}
    
    def get_industry_pe(self, industry):
        """获取行业估值（示例，实际可调用AKShare行业PE）"""
        # 简化：使用预定义行业估值分位（需定期更新）
        # 实际应该从AKShare获取行业指数估值
        valuation_map = {
            "新能源": 0.7,   # 70%分位，偏高
            "半导体": 0.8,
            "消费": 0.6,
            "医药": 0.4,
            "金融": 0.2,
            "周期": 0.5,
            "科技": 0.75,
            "军工": 0.5,
            "地产": 0.1,
            "农业": 0.3
        }
        return valuation_map.get(industry, 0.5)
    
    def get_industry_growth(self, industry):
        """行业盈利增长预期（简化）"""
        growth_map = {
            "新能源": 0.25,
            "半导体": 0.20,
            "消费": 0.10,
            "医药": 0.15,
            "金融": 0.05,
            "周期": 0.08,
            "科技": 0.22,
            "军工": 0.12,
            "地产": -0.05,
            "农业": 0.07
        }
        return growth_map.get(industry, 0.08)
    
    def get_industry_momentum(self, industry):
        """行业近期动量（模拟，可接入真实涨跌幅）"""
        # 这里简化，实际可调用AKShare获取行业指数20日涨跌幅
        momentum_map = {
            "新能源": 0.05,
            "半导体": -0.02,
            "消费": 0.03,
            "医药": -0.01,
            "金融": 0.02,
            "周期": 0.04,
            "科技": 0.06,
            "军工": 0.01,
            "地产": -0.03,
            "农业": 0.00
        }
        return momentum_map.get(industry, 0.00)
    
    def get_industry_liquidity(self, industry):
        """资金流向（北向资金、主力资金）简化"""
        # 可接入北向资金行业流向
        liquidity_map = {
            "新能源": 0.1,
            "半导体": 0.05,
            "消费": 0.08,
            "医药": 0.02,
            "金融": 0.03,
            "周期": 0.04,
            "科技": 0.06,
            "军工": -0.01,
            "地产": -0.05,
            "农业": 0.00
        }
        return liquidity_map.get(industry, 0.00)
    
    def score_industry(self, industry, event_sentiment_score=None):
        """计算行业综合得分（0~1）"""
        # 因子值归一化到0~1
        val = 1 - self.get_industry_pe(industry)  # 估值越低越好
        growth = self.get_industry_growth(industry)
        mom = self.get_industry_momentum(industry) + 0.5  # 平移
        liq = self.get_industry_liquidity(industry) + 0.5
        # 事件情感因子（外部传入）
        sent = event_sentiment_score if event_sentiment_score is not None else 0.5
        
        # 加权
        score = (self.weights["valuation"] * val +
                 self.weights["growth"] * growth +
                 self.weights["momentum"] * mom +
                 self.weights["sentiment"] * sent +
                 self.weights["liquidity"] * liq)
        return min(max(score, 0), 1)
    
    def recommend_industries(self, events, top_k=3):
        """根据事件列表推荐行业"""
        industry_scores = {}
        for evt in events:
            industries = evt.get("industries", [])
            sentiment_score = evt.get("sentiment_score", 0.5)
            # 情感强度：positive 加分，negative 减分
            if evt.get("sentiment") == "positive":
                sentiment_factor = 0.5 + sentiment_score * 0.5
            elif evt.get("sentiment") == "negative":
                sentiment_factor = 0.5 - sentiment_score * 0.5
            else:
                sentiment_factor = 0.5
            for ind in industries:
                base_score = self.score_industry(ind, sentiment_factor)
                industry_scores[ind] = industry_scores.get(ind, 0) + base_score
        # 平均分
        for ind in industry_scores:
            industry_scores[ind] /= len([e for e in events if ind in e.get("industries", [])]) if industry_scores[ind] > 0 else 1
        sorted_inds = sorted(industry_scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_inds[:top_k]
