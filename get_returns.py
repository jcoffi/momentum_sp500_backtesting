import sqlalchemy
import pandas as pd
from datetime import datetime
import numpy as np
from choose_stocks import choose_stocks

def get_date_series():
    """Returns a series of dates which are the last date in
    every month"""
    engine = sqlalchemy.create_engine("sqlite:///backtesting.db")
    with engine.connect() as con:
        mtl = pd.read_sql('monthlyReturns', con=con, index_col='Date')
    date_series = mtl.index.to_series().reset_index(drop=True)
    return date_series

def get_next_date(date, date_series=None):
    """Function to be used by get_returns() function.  Can
    Also be called alone.  The difference is, if called
    independently, it will create date series on its own
    but if called within get_returns(), date_series 
    will be passed in"""
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')
    if date_series is None:
        date_series = get_date_series()
    current_idx = date_series[date_series==date].index[0]
    return date_series[current_idx+1]

def get_returns(
    date, date_series=None, sel_1_portion = 0.5,
    sel_2_size=100, sel_3_size=10):
    """Get returns for a given date when using the
    momentum strategy.

    If you are only calculating for one date, you can 
    skip passing in a date series.
    
    If you are calculating returns for multiple
    dates, use date_series=get_date_series()
    outside of the function and pass it in for each
    date.  This will avoid generating a new date_series
    for repeated function calls"""
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')
    if date_series is None:
        date_series = get_date_series()
    next_date = get_next_date(date, date_series)
    engine = sqlalchemy.create_engine("sqlite:///backtesting.db")
    with engine.connect() as con:
        mtl = pd.read_sql('monthlyReturns', con=con, index_col='Date')
    chosen = choose_stocks(
        date, sel_1_portion, sel_2_size, sel_3_size)
    month_returns = mtl[chosen].loc[next_date]
    return month_returns

def get_returns_given_stocks(
    stocks, date, date_series=None):
    """Get returns for all stocks in a given list, 
    rather than selecting based on momentum strategy

    If you are only calculating for one date, you can 
    skip passing in a date series.
    
    If you are calculating returns for multiple
    dates, use date_series=get_date_series()
    outside of the function and pass it in for each
    date.  This will avoid generating a new date_series
    for repeated function calls"""
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')
    if date_series is None:
        date_series = get_date_series()
    next_date = get_next_date(date, date_series)
    engine = sqlalchemy.create_engine("sqlite:///backtesting.db")
    with engine.connect() as con:
        mtl = pd.read_sql('monthlyReturns', con=con, index_col='Date')
    month_returns = mtl[stocks].loc[next_date]
    return month_returns