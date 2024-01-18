from choose_stocks import get_sp500_date
import sqlalchemy
import pandas as pd
from datetime import datetime as dt
from get_returns import get_date_series

def main():

    def create_daily_mask():
        """Returns a mask of tickers which are in the sp500 for a given day"""
        engine = sqlalchemy.create_engine("sqlite:///backtesting.db")
        with engine.connect() as con:
            changes_df = pd.read_sql("changes_df", con=con, index_col='Date')
            ticker_df = pd.read_sql("ticker_df", con=con)
        # col_names is all the unique ticker names that are currently or have ever been
        # in the sp500 according to wikipedia
        col_names = pd.concat([changes_df.Added, changes_df.Removed, ticker_df.Symbol]).unique()
        date_index = pd.date_range(start='1/1/1999', end=dt.now())
        mask_df = pd.DataFrame(index=date_index, columns = col_names).drop(columns=[None])
        reversed = mask_df.iloc[::-1].copy()
        for row in reversed.iterrows():
            reversed.loc[row[0]] = row[1].index.isin(get_sp500_date(row[0]))
        mask_df = reversed.iloc[::-1].copy()
        with engine.connect() as con:
            mask_df.to_sql("daily_mask", con=con, index_label='Date', if_exists='replace')

    def create_monthly_mask():
        """For each date in date series, this function gets a mask of the
        stocks in the sp500 at that date, based on the previously createed
        daily_mask_df dataframe"""
        date_series = get_date_series()
        engine = sqlalchemy.create_engine("sqlite:///backtesting.db")
        with engine.connect() as con:
            daily_mask = pd.read_sql("daily_mask", con=con, index_col='Date')
        # Create empty monthly_mask dataframe with columns equal to daily_mask_df
        monthly_mask = pd.DataFrame(columns=daily_mask.columns)
        # Pulling out the original index to use later
        original_index = date_series.to_list()
        for date in date_series:
            # For a given date in date series, we lookup that date or the
            # closest date before it in daily_mask and add that row to the
            # monthly mask dataframe
            indexer = daily_mask.index.get_indexer(pd.DatetimeIndex([date]), method='pad')
            monthly_mask = pd.concat([
                monthly_mask,
                daily_mask.iloc[indexer].copy()]).astype(bool)
        # Now, we add the original index back to the dataframe, because we
        # the point of this mask is to see which dates are in a dataframe at
        # the end of a given month, even if that was not a stock market day
        mapper = dict(zip(monthly_mask.index, date_series.values))
        monthly_mask.rename(index=mapper, inplace=True)
        with engine.connect() as con:
            monthly_mask.to_sql('monthly_mask', con=con, index_label='Date', if_exists='replace')

    create_daily_mask()
    create_monthly_mask()

if __name__ == '__main__':
    main()
