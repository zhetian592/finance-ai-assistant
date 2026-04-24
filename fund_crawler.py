import json
import logging
import os
from datetime import datetime
import pandas as pd

from data_fetcher import fetch_all_sector_data, fetch_news
from event_extractor import process_news_batch
from sector_rotator import compute_sector_scores
from llm_client import llm_chat

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载ETF映射表
ETF_MAP_PATH = os.path.join(os.path.dirname(__file__), 'sector_etf_map.json')
if os.path.exists(ETF_MAP_PATH):
    with open(ETF_MAP_PATH, 'r', encoding='utf-8') as f:
        SECTOR_ETF_MAP = json.load(f)
else:
    logger.warning("sector_etf_map.json 不存在，ETF代码将为空")
    SECTOR_ETF_MAP = {}

def generate_report(mode='recommend', total_cash=50000):
    logger.info(f"开始生成报告，模式={mode}，可用资金={total_cash}")

    # 1. 获取行业数据
    sector_data = fetch_all_sector_data()

    # 2. 获取新闻并计算情绪得分
    headlines = fetch_news()
    sentiment_map = process_news_batch(headlines)

    # 3. 计算综合得分
    score_df = compute_sector_scores(sector_data, sentiment_map)
    top3 = score_df.head(3)

    # 4. 生成报告（不拼接新闻列表）
    report = f"📊 财经AI决策报告\n"
    report += f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    report += f"运行模式: {mode}\n"
    report += f"可用现金: {total_cash} 元\n\n"

    report += "🌟 行业轮动基金推荐\n"
    recommended = []
    for _, row in top3.iterrows():
        sec = row['sector']
        etf = SECTOR_ETF_MAP.get(sec, {})
        code = etf.get('code', '')
        name = etf.get('name', '无对应ETF')

        if not code:
            logger.warning(f"{sec} 无对应ETF，跳过")
            continue

        amount = total_cash // len(top3)
        report += f"{code} {name} 建议买入 {amount}元 (得分 {row['total_score']:.1f})\n"
        recommended.append(f"{sec}({code})")

    # 5. AI风控审核
    try:
        if recommended:
            prompt = f"""你是风控官。今日推荐行业：{', '.join(recommended)}。
请以最悲观的视角，指出这个配置当前面临的最大单一风险。回答控制在100字以内。"""
            risk_text, model = llm_chat([{"role": "user", "content": prompt}], temperature=0.2)
            if risk_text:
                report += f"\n⚠️ AI风控审核 ({model})：\n{risk_text}\n"
            else:
                report += "\n⚠️ AI风控暂不可用，请人工复核。\n"
    except Exception as e:
        logger.error(f"风控审核失败: {e}")
        report += "\n⚠️ AI风控暂不可用，请人工复核。\n"

    # 输出
    print(report)
    with open("今日推荐.txt", "w", encoding="utf-8") as f:
        f.write(report)

    logger.info("报告生成完毕")

if __name__ == "__main__":
    generate_report()
