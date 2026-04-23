import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import atexit

logger = logging.getLogger(__name__)

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
# 估值因子
# ------------------------------------------------------------
def get_sector_valuation(sector_name: str, lookback_years: int = 5):
    try:
        code = SECTORS.get(sector_name)
        if not code:
            logger.warning(f"{sector_name} 无对应指数代码")
            return None, False
        if not _login_baostock():
            return None, False
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=lookback_years*365)).strftime('%Y-%m-%d')
        rs = bs.query_history_k_data_plus(code, "date,peTTM", start, end, "d", "3")
        if rs.error_code != '0':
            logger.warning(f"{sector_name} 估值查询失败: {rs.error_msg}")
            return None, False
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if len(data) < 100:
            logger.warning(f"{sector_name} 历史数据不足 {len(data)} 天")
            return None, False
        df = pd.DataFrame(data, columns=rs.fields)
        df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
        df = df.dropna()
        if df.empty:
            return None, False
        cur_pe = df.iloc[-1]['peTTM']
        percentile = (df['peTTM'] < cur_pe).mean() * 100
        score = 100 - percentile
        # 异常值警告
        if sector_name == "建筑材料" and score > 90:
            logger.warning(f"⚠️ {sector_name} 估值得分 {score:.1f}，请注意水泥价格同比下跌、需求疲软的基本面风险")
        logger.info(f"[估值] {sector_name}: PE={cur_pe:.2f}, 分位={percentile:.1f}%, 得分={score:.1f}")
        return score, True
    except Exception as e:
        logger.warning(f"估值计算异常 {sector_name}: {e}")
        return None, False

# ------------------------------------------------------------
# 资金因子（北向资金行业流向）
# ------------------------------------------------------------
def get_sector_money_flow(sector_name: str):
    try:
        df = ak.stock_hsgt_industry_em()
        if df is None or df.empty:
            raise ValueError("Empty data from akshare")
        # 模糊匹配行业名称
        matched = df[df['行业'].str.contains(sector_name, na=False)]
        if matched.empty:
            # 尝试去除尾部"制造"等词
            short = sector_name.replace("制造", "").replace("材料", "")
            matched = df[df['行业'].str.contains(short, na=False)]
        if matched.empty:
            logger.debug(f"北向资金未找到行业 {sector_name}")
            return None, False
        net = matched['近5日净买额(万元)'].iloc[0] / 10000
        score = np.clip((net + 20) / 40 * 100, 0, 100)
        logger.info(f"[资金] {sector_name}: 净流入{net:.1f}亿, 得分={score:.1f}")
        return score, True
    except Exception as e:
        logger.warning(f"北向资金接口失败 {sector_name}: {e}")
        return None, False

# ------------------------------------------------------------
# 动量因子（基于行业指数收盘价）
# ------------------------------------------------------------
def get_sector_momentum(sector_name: str, days: int = 20):
    try:
        code = SECTORS.get(sector_name)
        if not code:
            return None, False
        if not _login_baostock():
            return None, False
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=days+10)).strftime('%Y-%m-%d')
        rs = bs.query_history_k_data_plus(code, "date,close", start, end, "d", "2")
        if rs.error_code != '0':
            logger.warning(f"{sector_name} 动量查询失败: {rs.error_msg}")
            return None, False
        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if len(data) < days+1:
            logger.debug(f"{sector_name} 动量数据不足: 仅{len(data)}条")
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
# 情绪因子（基于新闻事件）
# ------------------------------------------------------------
def get_sector_sentiment(sector_name: str, news_events: list):
    if not news_events:
        return None, False
    # 扩展行业关键词映射（确保覆盖常见新闻主题）
    sector_keywords = {
        "建筑材料": ["水泥", "玻璃", "建材", "基建", "地产开工"],
        "银行": ["银行", "降息", "信贷", "不良", "股息", "LPR", "存款利率"],
        "非银金融": ["券商", "保险", "信托", "证券", "资本市场"],
        "电子": ["芯片", "半导体", "消费电子", "AI", "算力", "存储"],
        "电气设备": ["新能源", "光伏", "风电", "电池", "电力", "电网"],
        "汽车": ["汽车", "新能源车", "自动驾驶", "整车", "零部件"],
        "食品饮料": ["白酒", "食品", "饮料", "消费", "调味品"],
        "医药生物": ["医药", "生物", "疫苗", "创新药", "CXO", "医疗器械"],
    }
    keywords = sector_keywords.get(sector_name, [sector_name])
    scores = []
    for evt in news_events:
        title = evt.get('title', '')
        summary = evt.get('summary', '')
        text = (title + " " + summary).lower()
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
# 综合数据获取
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
        
        # 有效因子数量不足2时，总分强制为50（避免单一因子驱动）
        if len(valid_scores) >= 2:
            total_score = np.mean(valid_scores)
        elif len(valid_scores) == 1:
            total_score = 50.0
            logger.warning(f"{sector} 仅有一个有效因子({valid_scores[0]:.1f})，总分设为50")
        else:
            total_score = 50.0
        
        rows.append({
            "sector": sector,
            "valuation_score": val if val_ok else None,
            "money_flow_score": money if money_ok else None,
            "momentum_score": mom if mom_ok else None,
            "sentiment_score": sent if sent_ok else None,
            "total_score": total_score,
            "effective_count": len(valid_scores)
        })
    df = pd.DataFrame(rows)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df
