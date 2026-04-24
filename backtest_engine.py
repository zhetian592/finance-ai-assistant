import backtrader as bt
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta
import os
import numpy as np

# ---------- 策略参数 ----------
TOP_N = 3
REBALANCE_WEEKDAY = 0      # 周一调仓 (0=周一, 6=周日)
START_DATE = '2021-06-01'
END_DATE = '2025-06-01'
INITIAL_CASH = 50000
COMMISSION = 0.0003

# ---------- 行业指标映射（需与 data_fetcher 中一致） ----------
SECTOR_CODE_MAP = {
    '食品饮料': 'sh.000807', '医药生物': 'sh.000808', '银行': 'sh.000134',
    '电子': 'sh.000066', '计算机': 'sh.000068', '建筑材料': 'sh.000150',
    '轻工制造': 'sh.000159', '非银金融': 'sh.000849', '汽车': 'sh.000941',
    '机械设备': 'sh.000109', '电气设备': 'sh.000106',
}

# ---------- 数据下载函数 ----------
def download_sector_data(data_dir='./data'):
    """下载所有行业指数日线数据到本地CSV（若不存在）"""
    os.makedirs(data_dir, exist_ok=True)
    bs.login()
    for sector, code in SECTOR_CODE_MAP.items():
        filepath = os.path.join(data_dir, f"{sector}.csv")
        if os.path.exists(filepath):
            continue
        print(f"下载 {sector} ({code}) ...")
        rs = bs.query_history_k_data_plus(
            code, "date,open,high,low,close,volume",
            start_date=START_DATE, end_date=END_DATE,
            frequency="d", adjustflag="2"
        )
        data = []
        while (rs.error_code == '0') and rs.next():
            data.append(rs.get_row_data())
        if not data:
            continue
        df = pd.DataFrame(data, columns=rs.fields)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        for col in ['open','high','low','close','volume']:
            df[col] = pd.to_numeric(df[col])
        df.to_csv(filepath)
    bs.logout()

# ---------- 因子计算函数（简化版） ----------
def calc_valuation_score(pe_percentile):
    if pe_percentile is None:
        return 50.0
    return 100 - pe_percentile

def calc_momentum_score(close_series, period=20):
    if close_series is None or len(close_series) < period + 1:
        return 50.0
    ret = (close_series.iloc[-1] - close_series.iloc[-period]) / close_series.iloc[-period]
    return np.clip((ret + 0.1) / 0.3 * 100, 0, 100)

def build_factor_df(price_dict, start, end):
    """基于收盘价计算估值分位和动量，生成得分表（资金、情绪用中性50）"""
    dates = pd.date_range(start, end, freq='B')
    records = []
    for dt in dates:
        for sector, df in price_dict.items():
            if dt not in df.index:
                continue
            close_series = df.loc[:dt, 'close']
            # 估值分位：用过去250天收盘价的高低位置模拟PE分位
            lookback = 250
            if len(close_series) >= lookback:
                hist = close_series.iloc[-lookback:-1]
                current = close_series.iloc[-1]
                pe_percentile = (hist < current).mean() * 100
            else:
                pe_percentile = 50.0
            val = calc_valuation_score(pe_percentile)
            mom = calc_momentum_score(close_series)
            money = 50.0   # 资金因子暂用中性
            sent = 50.0    # 情绪因子暂用中性
            total = (val + mom + money + sent) / 4
            records.append([dt, sector, total])
    df = pd.DataFrame(records, columns=['date', 'sector', 'total_score'])
    df.set_index(['date', 'sector'], inplace=True)
    return df

# ---------- 策略定义 ----------
class SectorRotation(bt.Strategy):
    params = (('factor_df', None),)

    def __init__(self):
        # 安全获取所有唯一的行业名称字符串列表
        self.sector_names = sorted(self.p.factor_df.index.get_level_values('sector').unique().tolist())
        # 构建 daily_scores 字典：{date: {sector: score}}
        self.daily_scores = {}
        for dt, group in self.p.factor_df.groupby(level='date'):
            scores = {}
            for (date_val, sector), val in group['total_score'].items():
                scores[sector] = val
            self.daily_scores[dt.date()] = scores

    def next(self):
        dt = self.datas[0].datetime.date(0)
        if dt.weekday() != REBALANCE_WEEKDAY:  # 只在指定工作日调仓
            return
        scores = self.daily_scores.get(dt)
        if scores is None:
            return

        # 选择得分最高的 TOP_N 行业
        sorted_sec = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_sectors = [s for s, _ in sorted_sec[:TOP_N]]

        # 平掉不在 top_sectors 的持仓
        for sec in self.sector_names:
            data = self.getdatabyname(sec)
            if self.getposition(data).size > 0 and sec not in top_sectors:
                self.close(data=data)

        # 等权买入 top_sectors
        if not top_sectors:
            return
        target_val = self.broker.getvalue() / len(top_sectors)
        for sec in top_sectors:
            data = self.getdatabyname(sec)
            price = data.close[0]
            size = int(target_val / price)
            self.order_target_size(data=data, target=size)

# ---------- 回测主函数 ----------
def run_backtest():
    # 1. 确保数据已下载
    download_sector_data()
    price_dict = {}
    for f in os.listdir('./data'):
        sector = f.replace('.csv', '')
        df = pd.read_csv(f'./data/{f}', index_col=0, parse_dates=True)
        price_dict[sector] = df

    # 2. 计算因子得分表
    factor_df = build_factor_df(price_dict, START_DATE, END_DATE)

    # 3. 初始化 Cerebro
    cerebro = bt.Cerebro()
    cerebro.addstrategy(SectorRotation, factor_df=factor_df)

    # 4. 添加数据 feeds（名称必须与策略中 sector_names 一致）
    for sector, df in price_dict.items():
        data = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(data, name=sector)

    cerebro.broker.setcash(INITIAL_CASH)
    cerebro.broker.setcommission(commission=COMMISSION)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

    print(f'初始资金: {cerebro.broker.getvalue():.2f}')
    results = cerebro.run()
    strat = results[0]

    final_value = cerebro.broker.getvalue()
    print(f'最终资金: {final_value:.2f}')
    print(f'年化收益率: {strat.analyzers.returns.get_analysis()["rnorm100"]:.2f}%')
    print(f'最大回撤: {strat.analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2f}%')
    print(f'夏普比率: {strat.analyzers.sharpe.get_analysis()["sharperatio"]:.2f}')

    # 保存结果
    with open('backtest_result.txt', 'w') as f:
        f.write(f'初始资金: {INITIAL_CASH:.2f}\n')
        f.write(f'最终资金: {final_value:.2f}\n')
        f.write(f'年化收益率: {strat.analyzers.returns.get_analysis()["rnorm100"]:.2f}%\n')
        f.write(f'最大回撤: {strat.analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2f}%\n')
        f.write(f'夏普比率: {strat.analyzers.sharpe.get_analysis()["sharperatio"]:.2f}\n')

if __name__ == '__main__':
    run_backtest()
