import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import atexit

logger = logging.getLogger(__name__)

# 申万一级行业 → baostock 指数代码（中证行业指数）
SECTORS = {
    "农林牧渔": "sz.399110", "采掘": "sz.399120", "化工": "sz.399130",
    "钢铁": "sz.399140", "有色金属": "sz.399150", "电子": "sz.399160",
    "家用电器": "sz.399170", "食品饮料": "sz.399180", "纺织服装": "sz.399190",
    "医药生物": "sz.399200", "公用事业": "sz.399210", "交通运输": "sz.399220",
    "房地产": "sz.399230", "商业贸易": "sz.399240", "休闲服务": "sz.399250",
    "计算机": "sz.399260", "传媒": "sz.399270", "通信": "sz.399280",
    "国防军工": "sz.399290", "银行": "sh.000134", "非银金融": "sz.399310",
    "汽车": "sz.399320", "机械设备": "sz.399330", "建筑装饰": "sz.399340",
    "电气设备": "sz.399350", "轻工制造": "sz.399360", "建筑材料": "sz.399370",
    "综合": "sz.399380",
}

_baostock_logged_in = False

def _login_baostock():
    global _baostock_logged_in
    if not _baostock_logged_in:
        try:
            lg = bs.login()
            if lg.error_code == '0':
                _baostock_logged_in = True
                logger.info("Baostock login success")
            else:
                logger.error(f"Baostock login failed: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"Baostock login exception: {e}")
            return False
    return True

atexit.register(lambda: bs.logout() if _baostock_logged_in else None)

# ------------------------------------------------------------
# 估值因子（数据不足或PE异常时返回 None）
# ------------------------------------------------------------
def get_sector_valuation(sector_name: str, lookback_years: int = 5):
    code = SECTORS.get(sector_name)
    if not code:
        return None, False
    if not _login_baostock():
        return None, False
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=lookback_years*365)).strftime('%Y-%m-%d')
    try:
        rs = bs.query_history_k_data_plus(code, "date,peTTM", start, end, "d", "3")
        if rs.error_code != '0':
            return None, False
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if len(data) < 100:
            return None, False
        df = pd.DataFrame(data, columns=rs.fields)
        df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
        df = df.dropna()
        if df.empty or (df['peTTM'] == 0).all():
            return None, False
        cur_pe = df.iloc[-1]['peTTM']
        if cur_pe <= 0:
            return None, False
        percentile = (df['peTTM'] < cur_pe).mean() * 100
        score = 100 - percentile
        if sector_name == "建筑材料" and score > 90:
            logger.warning(f"⚠️ {sector_name} 估值得分 {score:.1f}，请注意基本面风险")
        logger.info(f"[估值] {sector_name}: PE={cur_pe:.2f}, 分位={percentile:.1f}%, 得分={score:.1f}")
        return score, True
    except Exception as e:
        logger.warning(f"估值计算异常 {sector_name}: {e}")
        return None, False

# ------------------------------------------------------------
# 资金因子（接口暂时失效，返回 None）
# ------------------------------------------------------------
def get_sector_money_flow(sector_name: str):
    logger.debug(f"北向资金接口已失效，跳过 {sector_name}")
    return None, False

# ------------------------------------------------------------
# 动量因子（基于 baostock 收盘价）
# ------------------------------------------------------------
def get_sector_momentum(sector_name: str, days: int = 20):
    code = SECTORS.get(sector_name)
    if not code:
        return None, False
    if not _login_baostock():
        return None, False
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days+10)).strftime('%Y-%m-%d')
    try:
        rs = bs.query_history_k_data_plus(code, "date,close", start, end, "d", "2")
        if rs.error_code != '0':
            return None, False
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if len(data) < days+1:
            return None, False
        df = pd.DataFrame(data, columns=rs.fields)
        df['close'] = pd.to_numeric(df['close'])
        df = df.sort_values('date')
        ret = (df['close'].iloc[-1] - df['close'].iloc[-days-1]) / df['close'].iloc[-days-1]
        score = np.clip((ret + 0.1) / 0.3 * 100, 0, 100)
        logger.info(f"[动量] {sector_name}: {days}日收益{ret:.2%}, 得分={score:.1f}")
        return score, True
    except Exception as e:
        logger.warning(f"动量计算异常 {sector_name}: {e}")
        return None, False

# ------------------------------------------------------------
# 情绪因子（基于简单规则，若无新闻返回 None）
# ------------------------------------------------------------
def get_sector_sentiment(sector_name: str, news_events: list):
    if not news_events:
        return None, False
    sector_keywords = {
        "建筑材料": ["水泥", "玻璃", "建材", "基建"],
        "银行": ["银行", "降息", "信贷", "股息"],
        "非银金融": ["券商", "保险", "证券"],
        "电子": ["芯片", "半导体", "AI", "消费电子"],
        "电气设备": ["新能源", "光伏", "风电", "电池"],
    }
    keywords = sector_keywords.get(sector_name, [sector_name])
    scores = []
    for evt in news_events:
        text = (evt.get('title', '') + " " + evt.get('summary', '')).lower()
        if any(kw in text for kw in keywords):
            sent = evt.get('sentiment', 'neutral')
            s = evt.get('sentiment_score', 0.5)
            if sent == 'positive':
                scores.append(s * 100)
            elif sent == 'negative':
                scores.append((1 - s) * 100)
            else:
                scores.append(50)
    if not scores:
        return None, False
    score = np.mean(scores)
    logger.info(f"[情绪] {sector_name}: 相关新闻{len(scores)}条, 得分={score:.1f}")
    return score, True

# ------------------------------------------------------------
# 综合数据获取（优化评分逻辑）
# ------------------------------------------------------------
def fetch_all_sector_data(sector_list: list, news_events: list) -> pd.DataFrame:
    rows = []
    for sector in sector_list:
        val, val_ok = get_sector_valuation(sector)
        money, money_ok = get_sector_money_flow(sector)
        mom, mom_ok = get_sector_momentum(sector)
        sent, sent_ok = get_sector_sentiment(sector, news_events)

        valid_scores = []
        if val_ok:
            valid_scores.append(val)
        if money_ok:
            valid_scores.append(money)
        if mom_ok:
            valid_scores.append(mom)
        if sent_ok:
            valid_scores.append(sent)

        # 优化：有效因子不足2个时，不再强制设为50，而是直接使用唯一有效因子（如果有）
        if len(valid_scores) >= 2:
            total_score = np.mean(valid_scores)
            quality = "good"
        elif len(valid_scores) == 1:
            total_score = valid_scores[0]
            quality = "single_factor"
            logger.info(f"{sector} 仅有一个有效因子({valid_scores[0]:.1f})，直接使用")
        else:
            total_score = 50.0
            quality = "none"

        rows.append({
            "sector": sector,
            "valuation_score": val if val_ok else None,
            "money_flow_score": money if money_ok else None,
            "momentum_score": mom if mom_ok else None,
            "sentiment_score": sent if sent_ok else None,
            "total_score": total_score,
            "effective_count": len(valid_scores),
            "quality": quality
        })
    df = pd.DataFrame(rows)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df
