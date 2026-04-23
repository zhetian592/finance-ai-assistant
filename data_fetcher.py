import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime, timedelta
from functools import lru_cache

logger = logging.getLogger(__name__)

# ==================== 行业配置 ====================
# 申万一级行业 → baostock 指数代码（使用中证行业指数）
SECTORS = {
    "农林牧渔": "sz.399110",
    "采掘": "sz.399120",
    "化工": "sz.399130",
    "钢铁": "sz.399140",
    "有色金属": "sz.399150",
    "电子": "sz.399160",
    "家用电器": "sz.399170",
    "食品饮料": "sz.399180",
    "纺织服装": "sz.399190",
    "医药生物": "sz.399200",
    "公用事业": "sz.399210",
    "交通运输": "sz.399220",
    "房地产": "sz.399230",
    "商业贸易": "sz.399240",
    "休闲服务": "sz.399250",
    "计算机": "sz.399260",
    "传媒": "sz.399270",
    "通信": "sz.399280",
    "国防军工": "sz.399290",
    "银行": "sh.000134",
    "非银金融": "sz.399310",
    "汽车": "sz.399320",
    "机械设备": "sz.399330",
    "建筑装饰": "sz.399340",
    "电气设备": "sz.399350",
    "轻工制造": "sz.399360",
    "建筑材料": "sz.399370",
    "综合": "sz.399380",
}

# ==================== baostock 连接管理 ====================
_baostock_logged_in = False

def _login_baostock():
    global _baostock_logged_in
    if not _baostock_logged_in:
        lg = bs.login()
        if lg.error_code == '0':
            _baostock_logged_in = True
            logger.info("Baostock login success")
        else:
            logger.error(f"Baostock login failed: {lg.error_msg}")
            return False
    return True

def _logout_baostock():
    global _baostock_logged_in
    if _baostock_logged_in:
        bs.logout()
        _baostock_logged_in = False

import atexit
atexit.register(_logout_baostock)

# ==================== 估值因子（PE历史分位） ====================
def get_sector_valuation(sector_name: str, lookback_years: int = 5) -> float:
    """
    使用 baostock 获取行业指数 PE-TTM 历史分位
    返回得分 0-100，分数越高表示越低估
    """
    code = SECTORS.get(sector_name)
    if code is None:
        logger.warning(f"行业 {sector_name} 无对应指数代码")
        return 50.0
    if not _login_baostock():
        return 50.0

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=lookback_years*365)).strftime('%Y-%m-%d')
    
    try:
        rs = bs.query_history_k_data_plus(
            code=code,
            fields="date,peTTM",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )
        if rs.error_code != '0':
            logger.error(f"Baostock query error for {sector_name}: {rs.error_msg}")
            return 50.0
        
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        if len(data_list) < 100:
            logger.warning(f"{sector_name} 历史数据不足 {len(data_list)} 天")
            return 50.0
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
        df = df.dropna(subset=['peTTM'])
        if df.empty:
            return 50.0
        
        current_pe = df.iloc[-1]['peTTM']
        percentile = (df['peTTM'] < current_pe).mean() * 100
        # 估值得分：越低估值得分越高
        score = 100 - percentile
        
        # 异常值校验：对于建筑材料等周期行业，若PE极低但需求疲软，给予额外警告
        if sector_name == "建筑材料" and score > 80:
            logger.warning(f"⚠️ {sector_name} 估值得分{score:.1f}，但需结合基本面（水泥价格同比-13.9%，需求负增长）谨慎判断")
        
        logger.info(f"[估值] {sector_name}: PE={current_pe:.2f}, 分位={percentile:.1f}%, 得分={score:.1f}")
        return score
    except Exception as e:
        logger.error(f"估值计算异常 {sector_name}: {e}")
        return 50.0

# ==================== 资金因子（北向资金行业流向） ====================
def get_sector_money_flow(sector_name: str) -> float:
    """
    获取北向资金近5日净流入（亿元），返回得分0-100
    使用 akshare 接口，带降级方案
    """
    try:
        # 尝试新接口名（akshare 可能更新）
        df = ak.stock_hsgt_industry_em()
        if df.empty:
            raise ValueError("Empty data")
        # 行业名称匹配（模糊匹配）
        matched = df[df['行业'].str.contains(sector_name, na=False)]
        if matched.empty:
            # 尝试去除"制造"等后缀匹配
            short_name = sector_name.replace("制造", "").replace("材料", "")
            matched = df[df['行业'].str.contains(short_name, na=False)]
        if matched.empty:
            logger.debug(f"北向资金未找到行业 {sector_name}")
            return 50.0
        
        # 获取近5日净买额（万元），转为亿元
        net = matched['近5日净买额(万元)'].iloc[0] / 10000
        # 线性映射：净流入 >20亿 -> 100分，<-20亿 -> 0分
        score = (net + 20) / 40 * 100
        score = np.clip(score, 0, 100)
        logger.info(f"[资金] {sector_name}: 北向净流入{net:.1f}亿, 得分={score:.1f}")
        return score
    except Exception as e:
        logger.warning(f"北向资金接口失败 {sector_name}: {e}, 使用中性值50")
        return 50.0

# ==================== 动量因子 ====================
def get_sector_momentum(sector_name: str, days: int = 20) -> float:
    """
    获取行业指数最近 days 日收益率，返回得分0-100
    """
    code = SECTORS.get(sector_name)
    if code is None:
        return 50.0
    if not _login_baostock():
        return 50.0
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days+5)).strftime('%Y-%m-%d')
    try:
        rs = bs.query_history_k_data_plus(
            code=code,
            fields="date,close",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2"
        )
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        if len(data_list) < days+1:
            logger.debug(f"{sector_name} 动量数据不足")
            return 50.0
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        df['close'] = pd.to_numeric(df['close'])
        df = df.sort_values('date')
        ret = (df['close'].iloc[-1] - df['close'].iloc[-days-1]) / df['close'].iloc[-days-1]
        # 假设收益率范围 -0.1 ~ 0.2，映射到0~100
        score = (ret + 0.1) / 0.3 * 100
        score = np.clip(score, 0, 100)
        logger.info(f"[动量] {sector_name}: {days}日收益{ret:.2%}, 得分={score:.1f}")
        return score
    except Exception as e:
        logger.error(f"动量计算异常 {sector_name}: {e}")
        return 50.0

# ==================== 情绪因子（基于新闻事件） ====================
def get_sector_sentiment(sector_name: str, news_events: list) -> float:
    """
    根据新闻事件计算行业情绪得分
    news_events: list of dict with keys ['topics', 'sentiment', 'sentiment_score']
    """
    if not news_events:
        return 50.0
    
    # 建立行业关键词映射（将申万行业映射到新闻主题关键词）
    sector_keywords = {
        "建筑材料": ["水泥", "玻璃", "建材", "基建"],
        "银行": ["银行", "降息", "信贷", "不良率", "股息"],
        "轻工制造": ["造纸", "家居", "包装", "出口"],
        "电子": ["芯片", "半导体", "消费电子", "AI", "算力"],
        "电气设备": ["新能源", "光伏", "风电", "电池", "电力"],
        # 可继续扩展...
    }
    keywords = sector_keywords.get(sector_name, [sector_name])
    
    scores = []
    for evt in news_events:
        topics = evt.get('topics', [])
        # 检查事件主题是否匹配行业
        matched = any(kw in str(topics) or kw in evt.get('title', '') for kw in keywords)
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
    score = np.mean(scores)
    logger.info(f"[情绪] {sector_name}: 相关新闻{len(scores)}条, 得分={score:.1f}")
    return score

# ==================== 综合数据获取 ====================
def fetch_all_sector_data(sector_list: list, news_events: list) -> pd.DataFrame:
    """获取所有行业的多因子数据，返回 DataFrame"""
    data = []
    for sector in sector_list:
        val_score = get_sector_valuation(sector)
        money_score = get_sector_money_flow(sector)
        mom_score = get_sector_momentum(sector)
        sent_score = get_sector_sentiment(sector, news_events)
        total_score = (val_score + money_score + mom_score + sent_score) / 4
        data.append({
            "sector": sector,
            "valuation_score": round(val_score, 1),
            "money_flow_score": round(money_score, 1),
            "momentum_score": round(mom_score, 1),
            "sentiment_score": round(sent_score, 1),
            "total_score": round(total_score, 1)
        })
    df = pd.DataFrame(data)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df
