import json
import logging
from datetime import datetime
from data_fetcher import fetch_all_sector_data, fetch_news
from event_extractor import process_news_batch
from sector_rotator import compute_sector_scores
from llm_client import llm_chat  # 风控审核

logger = logging.getLogger(__name__)

# 加载ETF映射
with open('sector_etf_map.json', 'r', encoding='utf-8') as f:
    SECTOR_ETF_MAP = json.load(f)

def generate_report():
    # 1. 获取原始数据
    sector_data = fetch_all_sector_data()
    # 2. 获取新闻并计算情绪得分
    headlines = fetch_news()
    sentiment_scores = process_news_batch(headlines)
    # 将情绪得分注入sector_rotator（可临时存储）
    for sec, sc in sentiment_scores.items():
        if sec in sector_data:
            sector_data[sec]['sentiment'] = sc
    # 3. 计算最终得分
    from data_fetcher import SECTOR_BAOSTOCK_MAP
    import baostock as bs
    bs.login()
    # 获取沪深300作为基准
    rs = bs.query_history_k_data_plus("sh.000300", "date,close",
                                      (datetime.now().replace(year=datetime.now().year-1)).strftime('%Y-%m-%d'),
                                      datetime.now().strftime('%Y-%m-%d'), "d", adjustflag="2")
    bench_close = None
    if rs.error_code == '0':
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if data:
            import pandas as pd
            df_bench = pd.DataFrame(data, columns=['date','close'])
            df_bench['close'] = pd.to_numeric(df_bench['close'])
            bench_close = df_bench['close']
    bs.logout()
    
    score_df = compute_sector_scores(sector_data, bench_close)
    top3 = score_df.head(3)
    
    # 4. 生成报告
    report = f"📊 财经AI决策报告\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    report += "🌟 行业轮动基金推荐\n"
    for _, row in top3.iterrows():
        sec = row['sector']
        etf_info = SECTOR_ETF_MAP.get(sec, {})
        code = etf_info.get('code', '无对应ETF')
        if code == '无对应ETF' or etf_info.get('skip', False):
            continue
        amount = 50000 // len(top3)
        report += f"{code} {etf_info['name']} 建议买入 {amount}元 (得分{row['total_score']:.1f})\n"
    
    # 5. AI风控审核（可选）
    try:
        audit_prompt = f"""以最悲观视角审视以下推荐：
        {top3[['sector','total_score']].to_markdown()}
        今日新闻：{headlines[:5]}
        请指出最大单一风险。"""
        risk_opinion, _ = llm_chat([{"role":"user","content":audit_prompt}], temperature=0.2)
        report += f"\n\n⚠️ AI风控审核：\n{risk_opinion}"
    except Exception as e:
        logger.error(f"风控审核失败: {e}")
    
    # 输出
    print(report)
    with open("今日推荐.txt", "w", encoding="utf-8") as f:
        f.write(report)

if __name__ == "__main__":
    generate_report()
