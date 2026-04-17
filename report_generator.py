# -*- coding: utf-8 -*-
"""
HTML 报告生成模块
"""

import os
from datetime import datetime
from jinja2 import Template
import logging

logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self.output_dir = 'output'
        self.template_dir = 'templates'
    
    def generate(self):
        """生成 output/index.html"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 获取数据
        news = self.db.get_latest_news(limit=30)
        market = self.db.get_latest_market()
        fund_flows = self.db.get_recent_fund_flow(days=3)
        
        # 准备模板上下文
        context = {
            'generated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'news_list': [],
            'market_summary': self.format_market(market),
            'fund_summary': self.format_fund_flows(fund_flows),
            'signals': []
        }
        
        for row in news:
            item = {
                'title': row['title'],
                'time': row['time'],
                'source': row['source'],
                'sentiment': row['sentiment'] or '未分析',
                'score': row['score'] or '-',
                'confidence': row['confidence'] or '-',
                'expectation': row['expectation'] or '-',
                'sectors': row['beneficial_sectors'] or ''
            }
            context['news_list'].append(item)
        
        # 简单信号生成（复用 signal_engine 逻辑，这里简化）
        context['signals'] = self.generate_signal_summary(news)
        
        # 加载模板
        template_path = os.path.join(self.template_dir, 'report_template.html')
        if not os.path.exists(template_path):
            # 使用内置模板
            template_str = self.default_template()
        else:
            with open(template_path, 'r', encoding='utf-8') as f:
                template_str = f.read()
        
        template = Template(template_str)
        html = template.render(context)
        
        with open(os.path.join(self.output_dir, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("报告已生成: output/index.html")
    
    def format_market(self, market_data):
        if not market_data:
            return "暂无数据"
        latest = market_data[0]
        return f"最新收盘 {latest['close']} (日期 {latest['date']})"
    
    def format_fund_flows(self, flows):
        north = [f for f in flows if f['type'] == 'north']
        sector = [f for f in flows if f['type'] == 'sector']
        return {
            'north_inflow': north[0]['value'] if north else None,
            'top_sectors': sector[:3]
        }
    
    def generate_signal_summary(self, news):
        signals = []
        for row in news:
            if row['score'] and row['score'] >= 8 and row['confidence'] and row['confidence'] >= 0.7:
                signals.append({
                    'title': row['title'][:50],
                    'score': row['score'],
                    'confidence': row['confidence'],
                    'expectation': row['expectation']
                })
        return signals
    
    def default_template(self):
        return """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>财经AI决策报告</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: auto; background: white; padding: 20px; border-radius: 8px; }
        h1 { color: #333; }
        .timestamp { color: #666; font-size: 0.9em; margin-bottom: 20px; }
        .card { background: #f9f9f9; border-left: 4px solid #2196F3; padding: 10px; margin: 10px 0; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #2196F3; color: white; }
        .signal-buy { color: green; font-weight: bold; }
        .signal-wait { color: orange; }
        .footer { margin-top: 30px; font-size: 0.8em; color: #999; text-align: center; }
    </style>
</head>
<body>
<div class="container">
    <h1>📊 财经AI决策辅助报告</h1>
    <div class="timestamp">生成时间：{{ generated_time }}</div>
    
    <h2>📈 市场概览</h2>
    <div class="card">上证指数：{{ market_summary }}</div>
    
    <h2>💡 今日信号</h2>
    {% if signals %}
        {% for sig in signals %}
        <div class="card signal-buy">
            🔔 买入信号：{{ sig.title }}<br>
            评分：{{ sig.score }} | 置信度：{{ sig.confidence }} | 预期差：{{ sig.expectation }}
        </div>
        {% endfor %}
    {% else %}
        <div class="card">暂无符合条件的买入信号。</div>
    {% endif %}
    
    <h2>📰 最新新闻分析</h2>
    <table>
        <tr><th>标题</th><th>时间</th><th>来源</th><th>情绪</th><th>评分</th><th>置信度</th></tr>
        {% for item in news_list %}
        <tr>
            <td>{{ item.title[:60] }}</td>
            <td>{{ item.time }}</td>
            <td>{{ item.source }}</td>
            <td>{{ item.sentiment }}</td>
            <td>{{ item.score }}</td>
            <td>{{ item.confidence }}</td>
        </tr>
        {% endfor %}
    </table>
    
    <div class="footer">
        本报告由AI自动生成，仅供参考，不构成投资建议。<br>
        个人学习使用，风险自担。
    </div>
</div>
</body>
</html>
        """
