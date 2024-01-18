import sqlalchemy
import pandas as pd
from datetime import datetime as dt, timedelta
import numpy as np


def get_non_missing(date):
    """Given a date, takes a subset of the historicalReturns df
    corresponding to the year preceeding the date, and returns all
    tickers where zero nan values are found in that time frame."""
    if isinstance(date, str):
        date = dt.strptime(date, '%Y-%m-%d')
    past = date - timedelta(days=365)
    engine = sqlalchemy.create_engine('sqlite:///backtesting.db')
    with engine.connect() as con:
        hist_df = pd.read_sql('marketCloseData', con=con, index_col='Date')
    date_subset_df = hist_df.loc[past:date]
    tickers = date_subset_df.loc[:, (
        date_subset_df.isna().sum()==0)].columns.to_list()
    return tickers

def get_sp500_date(date):
    """Function to get a list of tickers present in the sp500 at a given date"""
    # Convert string inputs to datetime
    if isinstance(date, str):
        date = dt.strptime(date, '%Y-%m-%d')
    engine = sqlalchemy.create_engine('sqlite:///backtesting.db')
    with engine.connect() as con:
        tickers = pd.read_sql('ticker_df', con=con).squeeze().to_list()
        changes_df = pd.read_sql('changes_df', con=con, index_col='Date')
    # Get a subset of the changes from today until the input date
    changes_subset = changes_df.loc[:date, :].copy()
    # Add and remove stocks until tickers has sp500 stocks from given date
    for row in changes_subset.itertuples():
        if row.Removed not in tickers:
            tickers.append(row.Removed)
        if row.Added in tickers:
            tickers.remove(row.Added)
    return tickers

def get_eligible(date):
    """Function to get tickers which are in the sp500 at a given
    date and have no nan entries for a year preceeding the given
    date."""
    # Convert string inputs to datetime
    if isinstance(date, str):
        date = dt.strptime(date, '%Y-%m-%d')
    non_na_set = set(get_non_missing(date))
    sp500_set = set(get_sp500_date(date))
    eligible = list(sp500_set.intersection(non_na_set))
    return eligible

def choose_stocks(date, sel_1_portion = 0.5, sel_2_size=100, sel_3_size=10):
    """Selects stocks for a given date by using the following methodology:
    1)  Of all sp500 stocks, select sel_1_portion percent of them with the
        highest returns over 12 months.  We use sel_1_portion rather than a
        number because the number of eligible stocks for a given date varies
        based on availability of data
    2)  Select sel_2_size stocks with highest returns over last 6 months
        from stocks chosen in step 1
    3)  Finally, select top sel_3_size stocks from subset created by step 2
    4)  At each step, I am only selecting stocks with positive returns over
        the given 12, 6 or 3 month period"""
    # Reading in 12, 6 and 3 month return dataframes into memory
    engine = sqlalchemy.create_engine('sqlite:///backtesting.db')
    with engine.connect() as con:
        ret_12 = pd.read_sql('12MonthReturns', con=con, index_col='Date')
        ret_6 = pd.read_sql('6MonthReturns', con=con, index_col='Date')
        ret_3 = pd.read_sql('3MonthReturns', con=con, index_col='Date')
    # Choosing stocks for 12 month, 6 month and 3 month periods
    eligible = get_eligible(date)
    selection_1_num = int(np.floor(len(eligible)*sel_1_portion))
    selection_1 = (ret_12
        .loc[date, eligible]
        .loc[lambda x: x>=1]
        .nlargest(n=selection_1_num)
        .index
        .to_list())
    selection_2 = (ret_6
        .loc[date, selection_1]
        .loc[lambda x: x>=1]
        .nlargest(n=sel_2_size)
        .index
        .to_list())
    selection_3 = (ret_3
        .loc[date, selection_2]
        .loc[lambda x: x>=1]
        .nlargest(n=sel_3_size)
        .index
        .to_list())
    return selection_3
