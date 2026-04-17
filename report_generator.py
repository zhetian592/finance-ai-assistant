# report_generator.py (替换原文件)

import os
import json
from datetime import datetime
from jinja2 import Template
import logging
from collections import Counter

logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self.output_dir = 'output'
        self.template_dir = 'templates'
    
    def generate(self):
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 获取数据
        news_rows = self.db.get_latest_news(limit=50)   # 取更多用于统计
        market = self.db.get_latest_market()
        fund_flows = self.db.get_recent_fund_flow(days=3)
        
        # 处理新闻列表 (显示最近30条)
        news_list = []
        scores = []
        sentiments = []
        for row in news_rows[:30]:
            item = {
                'title': row['title'],
                'time': row['time'],
                'source': row['source'],
                'sentiment': row['sentiment'] or '未分析',
                'score': row['score'] if row['score'] else '-',
                'confidence': round(row['confidence'], 2) if row['confidence'] else '-',
                'expectation': row['expectation'] or '-'
            }
            news_list.append(item)
            if row['score']:
                scores.append(row['score'])
            if row['sentiment']:
                sentiments.append(row['sentiment'])
        
        # 统计评分分布 (1-10)
        score_counter = Counter(scores)
        score_labels = [str(i) for i in range(1, 11)]
        score_counts = [score_counter.get(i, 0) for i in range(1, 11)]
        
        # 情绪占比
        sentiment_counter = Counter(sentiments)
        sentiment_data = [
            {'name': k, 'value': v} for k, v in sentiment_counter.items() if k
        ]
        if not sentiment_data:
            sentiment_data = [{'name': '无数据', 'value': 1}]
        
        # 信号生成 (复用原有逻辑，这里简化直接从高分新闻生成)
        signals = []
        for row in news_rows:
            if (row['score'] and row['score'] >= 8 and 
                row['confidence'] and row['confidence'] >= 0.7 and
                row['expectation'] in ['超预期', '部分预期']):
                # 计算建议仓位 (简单公式)
                position = round((row['score']/10) * row['confidence'] * 0.05 * 100, 1)
                signals.append({
                    'title': row['title'][:60],
                    'score': row['score'],
                    'confidence': round(row['confidence'], 2),
                    'expectation': row['expectation'],
                    'position': position
                })
        
        # 北向资金 (最近一条)
        north_val = None
        for f in fund_flows:
            if f['type'] == 'north' and f['value']:
                north_val = f['value']
                break
        north_flow_str = f"{north_val:.2f}亿" if north_val else "暂无"
        
        # 平均评分
        avg_score = round(sum(scores)/len(scores), 1) if scores else 0
        avg_confidence = round(sum([c for c in [row['confidence'] for row in news_rows if row['confidence']]]) / len([c for c in [row['confidence'] for row in news_rows if row['confidence']]]), 2) if any(row['confidence'] for row in news_rows) else 0
        
        context = {
            'generated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'news_list': news_list,
            'signals': signals,
            'signals_count': len(signals),
            'news_count': len(news_rows),
            'avg_score': avg_score,
            'avg_confidence': avg_confidence,
            'north_flow_str': north_flow_str,
            'north_flow': north_val if north_val else 0,
            'score_labels': json.dumps(score_labels),
            'score_counts': json.dumps(score_counts),
            'sentiment_data': json.dumps(sentiment_data)
        }
        
        # 加载模板
        template_path = os.path.join(self.template_dir, 'report_template.html')
        if not os.path.exists(template_path):
            template_str = self.default_template()
        else:
            with open(template_path, 'r', encoding='utf-8') as f:
                template_str = f.read()
        
        template = Template(template_str)
        html = template.render(context)
        
        with open(os.path.join(self.output_dir, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("仪表盘报告已生成: output/index.html")
    
    def default_template(self):
        # 这里返回和上面HTML一样的内容，但为了文件完整性，直接返回字符串（省略，实际可复制上面HTML）
        return """<!DOCTYPE html>..."""  # 实际使用时请复制上面完整的HTML模板
