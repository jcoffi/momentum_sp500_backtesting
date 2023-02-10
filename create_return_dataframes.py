import numpy as np
import pandas as pd
import sqlalchemy
from get_returns import get_date_series, get_returns, get_returns_given_stocks
from choose_stocks import get_eligible

def main():
    def pad(x, max_num=None):
        x = np.pad(x, (0,max_num-len(x)))
        return x

    def replace_zeros(x):
        x = pd.Series(x).replace(0, np.nan)
        return x

    def create_returns_df():
        """Create a dataframe of returns if using the 
        momentum strategy"""
        date_series = get_date_series()
        rets = []
        for date in date_series[12:-2]:
            ret = get_returns(date, date_series)
            rets.append(ret)
        rets_padded = [replace_zeros(pad(ret, max_num=10)) for ret in rets]
        returns_df = pd.DataFrame(
            data = np.stack(rets_padded),
            index = date_series[12:-2])
        engine = sqlalchemy.create_engine('sqlite:///backtesting.db')
        with engine.connect() as con:
            returns_df.to_sql(
                'strategy_monthly_returns', con=con, if_exists='replace')

    def create_sp500_returns_df():
        """Create a dataframe of returns if using all 
        stocks in the sp500 for each given date"""
        date_series = get_date_series()
        rets = []
        for date in date_series[12:-2]:
            eligible = get_eligible(date)
            ret = get_returns_given_stocks(eligible, date, date_series)
            rets.append(ret)
        rets_padded = [replace_zeros(pad(ret, max_num=600)) for ret in rets]
        returns_df = pd.DataFrame(
            np.stack(rets_padded), 
            index = date_series[12:-2])
        returns_df = returns_df.dropna(axis=1, how='all')
        engine = sqlalchemy.create_engine('sqlite:///backtesting.db')
        with engine.connect() as con:
            returns_df.to_sql(
                'sp500_monthly_returns', con=con, if_exists='replace')

    create_returns_df()
    create_sp500_returns_df()

if __name__ == '__main__':
    main()