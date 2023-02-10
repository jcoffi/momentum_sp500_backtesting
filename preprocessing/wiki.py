## This notebook takes two wikipedia dataframes and processes 
# them into the base data needed to backtest my trading strategy

import pandas as pd
from datetime import datetime
import sqlalchemy

def main():

    # creates a ticker_df which contains
    def create_ticker_df():
        """Creates a dataframe with all stocks currently in 
        the sp500"""
        return (pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_"
            "companies#Selected_changes_to_the_list_of_S&P_500"
            "_components")[0]
            .drop(
            ['GICS Sector', 'GICS Sub-Industry',
            'Headquarters Location', 'Date added', 'CIK',
            'Founded', 'Security'], axis=1))


    def create_changes_df():
        """Create a dataframe of changes to the sp500"""
        def format_date(date):
            d = datetime.strptime(date, '%B %d, %Y')
            d = d.strftime('%Y-%m-%d')
            return d
        return (pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_"
            "companies#Selected_changes_to_the_list_of_S&P_500"
            "_components")[1]
            .drop(['Security', 'Reason'], axis=1, level=1)
            .droplevel(level=1, axis=1)
            .assign(Date=lambda df_: df_['Date'].apply(format_date))
            .astype({'Date':'datetime64[ns]'}))

    def to_sql():
        """creates ticker_df and changes_df and loads them 
        into the database"""
        ticker_df = create_ticker_df()
        changes_df = create_changes_df()
        engine = sqlalchemy.create_engine(
            "sqlite:///backtesting.db")
        with engine.connect() as con:
            ticker_df.to_sql('ticker_df', con=con, index=False)
            changes_df.to_sql('changes_df', con=con, index=False)

    to_sql()

if __name__ == "__main__":
    main()