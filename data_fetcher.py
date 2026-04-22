import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# 申万一级行业列表（28个）及对应ETF代码（示例，可扩展）
SECTORS = {
    "农林牧渔": "159825",
    "采掘": "510410",
    "化工": "516120",
    "钢铁": "515210",
    "有色金属": "512400",
    "电子": "159997",
    "家用电器": "159996",
    "食品饮料": "515170",
    "纺织服装": "512990",
    "医药生物": "512010",
    "公用事业": "159611",
    "交通运输": "159666",
    "房地产": "512200",
    "商业贸易": "516960",
    "休闲服务": "159766",
    "计算机": "512720",
    "传媒": "512980",
    "通信": "515880",
    "国防军工": "512710",
    "银行": "512800",
    "非银金融": "512880",
    "汽车": "516110",
    "机械设备": "516960",
    "建筑装饰": "516970",
    "电气设备": "516880",
    "轻工制造": "159938",
    "建筑材料": "516750",
    "综合": "512990"
}

def get_sector_pe_hist(sector_name: str, days: int = 250) -> pd.Series:
    """获取行业指数历史PE（近days天），返回Series，索引为日期"""
    # AKShare 暂无直接行业指数PE接口，使用行业指数日频数据估算
    # 此处使用 wind 行业指数（如 "801010.SI" 对应农林牧渔）
    # 简化：使用行业指数收盘价替代，真实场景需接入专业数据
    # 返回随机模拟数据（仅供演示），实际应替换为真实接口
    # TODO: 使用 tushare pro 或聚宽数据
    end = datetime.now()
    start = end - timedelta(days=days)
    # 模拟：生成一个随机游走的PE序列
    np.random.seed(hash(sector_name) % 10000)
    pe = np.random.normal(20, 5, days).cumsum() + 30
    dates = pd.date_range(start=end - timedelta(days=days-1), periods=days, freq='D')
    return pd.Series(pe, index=dates)

def get_sector_north_flow(sector_name: str) -> float:
    """获取北向资金最近5日净流入（亿元）"""
    try:
        # 北向资金行业流向接口（需较新akshare版本）
        df = ak.stock_hsgt_industry_em()
        if df.empty:
            return 0.0
        # 根据行业名称匹配
        row = df[df['行业'] == sector_name]
        if not row.empty:
            # 假设列名 '今日净买额(万元)' 或 '近5日净买额(万元)'
            if '近5日净买额(万元)' in row.columns:
                val = row['近5日净买额(万元)'].iloc[0]
                return float(val) / 10000  # 转换为亿元
        return 0.0
    except Exception as e:
        logger.warning(f"获取北向资金行业 {sector_name} 失败: {e}")
        return 0.0

def get_sector_momentum(sector_name: str, days: int = 20) -> float:
    """获取行业指数最近days日收益率（%）"""
    # 模拟实现：实际应使用行业指数历史行情
    # 使用ak.stock_zh_index_hist_em? 行业指数代码映射复杂，先返回0
    # 真实场景需维护行业指数代码映射表
    # 这里返回随机值（仅框架）
    np.random.seed(hash(sector_name + str(days)) % 10000)
    return np.random.uniform(-5, 10)

def get_sector_valuation_score(sector_name: str) -> float:
    """
    估值分位得分：基于当前PE在历史250日的位置，越低得分越高
    返回0~100分，100表示极度低估
    """
    pe_hist = get_sector_pe_hist(sector_name)
    if pe_hist.empty:
        return 50.0
    current_pe = pe_hist.iloc[-1]
    percentile = (pe_hist < current_pe).mean() * 100
    # 估值越低，得分越高：得分 = 100 - percentile
    return max(0, min(100, 100 - percentile))

def get_sector_money_flow_score(sector_name: str) -> float:
    """资金流得分：基于北向资金净流入"""
    net = get_sector_north_flow(sector_name)
    # 净流入>10亿得100分，>5亿得70，>0得50，<0得20，<-10亿得0
    if net >= 10:
        return 100.0
    elif net >= 5:
        return 70.0
    elif net >= 0:
        return 50.0
    elif net >= -10:
        return 20.0
    else:
        return 0.0

def get_sector_momentum_score(sector_name: str) -> float:
    """动量得分：最近20日收益率，越高得分越高"""
    ret = get_sector_momentum(sector_name, 20)
    # 假设收益率范围 -10% ~ 20%，线性映射到0~100
    score = (ret + 10) / 30 * 100
    return max(0, min(100, score))

def get_sector_sentiment_score(sector_name: str, news_events: list) -> float:
    """
    根据新闻事件（来自event_extractor）计算行业情绪得分
    news_events: list of dict with keys ['topics', 'sentiment', 'sentiment_score']
    """
    if not news_events:
        return 50.0
    # 找到与当前行业相关的事件
    scores = []
    for evt in news_events:
        topics = evt.get('topics', [])
        # 检查行业名称是否与主题匹配（简单关键词匹配）
        matched = False
        for topic in topics:
            if topic in sector_name or sector_name in topic:
                matched = True
                break
        if matched:
            sentiment = evt.get('sentiment', 'neutral')
            sent_score = evt.get('sentiment_score', 0.5)
            if sentiment == 'positive':
                scores.append(sent_score * 100)
            elif sentiment == 'negative':
                scores.append((1 - sent_score) * 100)
            else:
                scores.append(50)
    if not scores:
        return 50.0
    return np.mean(scores)

def fetch_all_sector_data(sector_list: list, news_events: list) -> pd.DataFrame:
    """获取所有行业的多因子数据，返回DataFrame"""
    data = []
    for sector in sector_list:
        pe_score = get_sector_valuation_score(sector)
        flow_score = get_sector_money_flow_score(sector)
        mom_score = get_sector_momentum_score(sector)
        sent_score = get_sector_sentiment_score(sector, news_events)
        # 综合得分（等权，可配置）
        total_score = (pe_score + flow_score + mom_score + sent_score) / 4
        data.append({
            "sector": sector,
            "etf_code": SECTORS.get(sector, ""),
            "valuation_score": pe_score,
            "money_flow_score": flow_score,
            "momentum_score": mom_score,
            "sentiment_score": sent_score,
            "total_score": total_score
        })
    df = pd.DataFrame(data)
    df = df.sort_values("total_score", ascending=False)
    return df
