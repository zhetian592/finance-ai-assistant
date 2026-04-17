# -*- coding: utf-8 -*-
"""
AI 分析模块：调用 DeepSeek API
"""

import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

class DeepSeekAnalyzer:
    def __init__(self, api_key: str, config: dict):
        self.client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com/v1"
        )
        self.model = config.get('model', 'deepseek-chat')
        self.temperature = config.get('temperature', 0.3)
        # 行业白名单（申万一级）
        self.industry_whitelist = {
            "农林牧渔", "食品饮料", "医药生物", "银行", "非银金融", "房地产",
            "建筑装饰", "建筑材料", "电气设备", "机械设备", "国防军工",
            "电子", "计算机", "通信", "传媒", "有色金属", "采掘", "化工",
            "钢铁", "家用电器", "汽车", "休闲服务", "商业贸易", "公用事业", "交通运输"
        }
        # ETF 白名单（示例）
        self.etf_whitelist = {
            "沪深300ETF", "中证500ETF", "创业板ETF", "科创50ETF",
            "证券ETF", "芯片ETF", "新能源车ETF", "消费ETF", "医药ETF",
            "军工ETF", "有色ETF", "银行ETF"
        }
    
    def analyze_news(self, news: dict) -> dict:
        prompt = f"""你是专业财经分析师。分析以下新闻，输出JSON。

新闻标题：{news['title']}
摘要：{news['summary']}
时间：{news['time']}
来源：{news['source']}

要求输出：
{{
    "sentiment": "利好/利空/中性",
    "affected_industries": ["行业1", "行业2"],
    "beneficial_sectors": ["板块1", "板块2"],
    "related_funds_stocks": ["ETF/股票参考1"],
    "score": 1-10,
    "confidence": 0-1,
    "expectation": "超预期/部分预期/已充分预期"
}}

注意：score定义 1-3重大利空 4-5轻度利空 6中性 7-8轻度利好 9-10重大利好。
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你只输出JSON格式，不要包含其他文字。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            # 幻觉校验
            result = self.validate_analysis(result)
            return result
        except Exception as e:
            logger.error(f"AI分析失败: {e}")
            return {
                "sentiment": "中性",
                "affected_industries": [],
                "beneficial_sectors": [],
                "related_funds_stocks": [],
                "score": 5,
                "confidence": 0.5,
                "expectation": "部分预期"
            }
    
    def validate_analysis(self, analysis: dict) -> dict:
        """幻觉校验：行业、板块、ETF 白名单过滤"""
        # 校验行业
        valid_industries = []
        for ind in analysis.get('affected_industries', []):
            if ind in self.industry_whitelist:
                valid_industries.append(ind)
        if not valid_industries:
            valid_industries = ["其他"]
        analysis['affected_industries'] = valid_industries
        
        # 校验板块
        valid_sectors = []
        for sec in analysis.get('beneficial_sectors', []):
            # 简化：板块名长度>1且不含特殊字符
            if len(sec) > 1 and not any(c in sec for c in '#@!'):
                valid_sectors.append(sec)
        if not valid_sectors:
            valid_sectors = ["待核实"]
        analysis['beneficial_sectors'] = valid_sectors
        
        # 校验基金/股票参考
        valid_refs = []
        for ref in analysis.get('related_funds_stocks', []):
            # 白名单或常见命名
            if ref in self.etf_whitelist or 'ETF' in ref or len(ref) > 2:
                valid_refs.append(ref)
        if not valid_refs:
            valid_refs = ["需人工确认"]
        analysis['related_funds_stocks'] = valid_refs
        
        # 确保 score 在 1-10
        score = analysis.get('score', 5)
        analysis['score'] = max(1, min(10, score))
        # confidence 0-1
        conf = analysis.get('confidence', 0.5)
        analysis['confidence'] = max(0, min(1, conf))
        return analysis
