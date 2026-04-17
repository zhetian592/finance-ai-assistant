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
        self.output_dir = 'output'

    def generate(self, signals=None, news_count=0, api_ok=True):
        os.makedirs(self.output_dir, exist_ok=True)
        rows = self.db.get_latest_news(limit=30)
        
        # 统计数据
        scores = [r['score'] for r in rows if r['score']]
        sentiments = [r['sentiment'] for r in rows if r['sentiment']]
        
        # 错误信息
        error_msg = ""
        if news_count == 0:
            error_msg = "⚠️ 新闻爬取失败（0条），请检查网络或数据源接口"
        elif not api_ok:
            error_msg = "⚠️ DeepSeek API Key 无效，AI 分析未执行"
        elif len(rows) == 0:
            error_msg = "⚠️ 数据库无新闻记录"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
        <title>财经AI决策报告</title>
        <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
        <style>
        body {{font-family:sans-serif;background:#f0f2f5;padding:20px}}
        .container {{max-width:1200px;margin:auto;background:white;border-radius:20px;padding:20px}}
        .kpi-grid {{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin-bottom:20px}}
        .kpi-card {{background:#f8fafc;border-radius:16px;padding:16px}}
        .kpi-value {{font-size:28px;font-weight:bold}}
        .error-banner {{background:#fee2e2;border-left:4px solid #ef4444;padding:12px;margin-bottom:20px;border-radius:8px}}
        .signal-card {{background:#e6f7e6;border-left:4px solid #10b981;padding:12px;margin:8px 0}}
        table {{width:100%;border-collapse:collapse}}
        th,td {{text-align:left;padding:8px;border-bottom:1px solid #ddd}}
        </style>
        </head>
        <body>
        <div class="container">
        <h1>📊 财经AI决策仪表盘</h1>
        <div>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        {f'<div class="error-banner">{error_msg}</div>' if error_msg else ''}
        <div class="kpi-grid">
            <div class="kpi-card">📡 今日信号<br><span class="kpi-value">{len(signals or [])}</span> 个</div>
            <div class="kpi-card">🎯 平均评分<br><span class="kpi-value">{sum(scores)/len(scores) if scores else 0:.1f}</span>/10</div>
            <div class="kpi-card">📰 新闻数<br><span class="kpi-value">{len(rows)}</span> 条</div>
        </div>
        <div id="scoreChart" style="height:300px"></div>
        <div id="sentimentChart" style="height:300px"></div>
        <h2>🔔 买入信号</h2>
        {"".join([f'<div class="signal-card">⭐ {s["title"]}<br>评分 {s["score"]} | 置信度 {s["confidence"]} | 建议仓位 {s["position"]}%</div>' for s in (signals or [])]) or "<div>无符合条件的信号</div>"}
        <h2>📰 新闻列表</h2>
        <table>
        <tr><th>标题</th><th>时间</th><th>情绪</th><th>评分</th></tr>
        {"".join([f"<tr><td>{r['title'][:60]}</td><td>{r['time']}</td><td>{r['sentiment'] or '未分析'}</td><td>{r['score'] or '-'}</td></tr>" for r in rows])}
        </table>
        <div style="text-align:center;margin-top:30px;color:#999">本报告仅个人学习使用，不构成投资建议</div>
        </div>
        <script>
        var scoreChart = echarts.init(document.getElementById('scoreChart'));
        scoreChart.setOption({{
            xAxis: {{ type: 'category', data: {json.dumps([str(i) for i in range(1,11)])} }},
            yAxis: {{ type: 'value' }},
            series: [{{ type: 'bar', data: {json.dumps([Counter(scores).get(i,0) for i in range(1,11)])} }}]
        }});
        var pieChart = echarts.init(document.getElementById('sentimentChart'));
        pieChart.setOption({{
            series: [{{ type: 'pie', data: {json.dumps([{'name':k,'value':v} for k,v in Counter(sentiments).items()] or [{'name':'无数据','value':1}])} }}]
        }});
        </script>
        </body>
        </html>
        """
        with open(os.path.join(self.output_dir, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("报告生成完成")
