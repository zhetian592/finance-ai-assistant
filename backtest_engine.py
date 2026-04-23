# backtest_engine.py
# 独立运行，回测行业轮动策略
import backtrader as bt
import pandas as pd
import numpy as np
import os
from glob import glob
from datetime import datetime, timedelta
import baostock as bs

# 行业映射（同 data_fetcher）
SECTORS = {
    "食品饮料": "sz.399180",
    "医药生物": "sz.399200",
    "电子": "sz.399160",
    "计算机": "sz.399260",
    "银行": "sh.000134",
    "非银金融": "sz.399310",
}

def download_hist_data(start='2021-01-01', end='2025-12-31', save_dir='./hist_data'):
    os.makedirs(save_dir, exist_ok=True)
    bs.login()
    for name, code in SECTORS.items():
        filepath = f"{save_dir}/{name}.csv"
        if os.path.exists(filepath):
            continue
        rs = bs.query_history_k_data_plus(code, "date,open,high,low,close,volume",
                                          start_date=start, end_date=end,
                                          frequency="d", adjustflag="2")
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if rows:
            df = pd.DataFrame(rows, columns=rs.fields)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            for col in ['open','high','low','close','volume']:
                df[col] = pd.to_numeric(df[col])
            df.to_csv(filepath)
    bs.logout()

def calc_valuation_score(prices, lookback=250):
    if len(prices) < lookback:
        return 50.0
    cur = prices.iloc[-1]
    hist = prices.iloc[-lookback:-1]
    percentile = (hist < cur).mean() * 100
    return 100 - percentile

def calc_momentum_score(prices, period=20):
    if len(prices) < period+1:
        return 50.0
    ret = (prices.iloc[-1] - prices.iloc[-period-1]) / prices.iloc[-period-1]
    return np.clip((ret + 0.1) / 0.3 * 100, 0, 100)

def build_factor_df(price_dict, start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq='B')
    records = []
    for dt in dates:
        for sector, df in price_dict.items():
            if dt not in df.index:
                continue
            prices = df.loc[:dt]['close']
            val = calc_valuation_score(prices)
            mom = calc_momentum_score(prices)
            total = (val + mom + 50 + 50) / 4  # 资金和情绪暂为中性
            records.append([dt, sector, total])
    df_factors = pd.DataFrame(records, columns=['date', 'sector', 'total_score'])
    df_factors.set_index(['date', 'sector'], inplace=True)
    return df_factors

class SectorRotationStrategy(bt.Strategy):
    params = (('top_n', 2), ('factor_df', None))

    def __init__(self):
        self.sector_names = list(self.p.factor_df.index.get_level_values('sector').unique())
        self.daily_scores = {}
        for dt, group in self.p.factor_df.groupby(level='date'):
            self.daily_scores[dt.date()] = group['total_score'].to_dict()

    def next(self):
        dt = self.datas[0].datetime.date(0)
        if dt.weekday() != 0:
            return
        if dt not in self.daily_scores:
            return
        scores = self.daily_scores[dt]
        sorted_sec = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_sectors = [sec for sec, _ in sorted_sec[:self.p.top_n]]

        for sec in self.sector_names:
            data = self.getdatabyname(sec)
            pos = self.getposition(data).size
            if pos > 0 and sec not in top_sectors:
                self.close(data=data)

        if not top_sectors:
            return
        target_val = self.broker.getvalue() / len(top_sectors)
        for sec in top_sectors:
            data = self.getdatabyname(sec)
            price = data.close[0]
            target_size = int(target_val / price)
            self.order_target_size(data=data, target=target_size)

def run_backtest():
    download_hist_data()
    price_dict = {}
    for f in glob("./hist_data/*.csv"):
        sector = os.path.basename(f).replace('.csv', '')
        df = pd.read_csv(f, index_col=0, parse_dates=True)
        price_dict[sector] = df
    df_factors = build_factor_df(price_dict, '2021-06-01', '2025-06-01')
    cerebro = bt.Cerebro()
    cerebro.addstrategy(SectorRotationStrategy, factor_df=df_factors)
    for sector, df in price_dict.items():
        data = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(data, name=sector)
    cerebro.broker.setcash(50000.0)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    print('初始资金: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    strat = results[0]
    print('最终资金: %.2f' % cerebro.broker.getvalue())
    print('年化收益率: %.2f%%' % (strat.analyzers.returns.get_analysis()['rnorm100']))
    print('最大回撤: %.2f%%' % strat.analyzers.drawdown.get_analysis()['max']['drawdown'])
    print('夏普比率: %.2f' % strat.analyzers.sharpe.get_analysis()['sharperatio'])

if __name__ == '__main__':
    run_backtest()
