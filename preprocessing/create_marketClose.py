import yfinance as yf
import pandas as pd
import datetime
import sqlalchemy

def main():

    def get_historical_changes(changes_df, date=datetime.datetime.now()):
        """Trims changes df to include only changes between 
        1999-01-01 and the current date, but date could be 
        set to any datetime.datetime object"""
        today = date
        return (changes_df.loc[
            (changes_df['Date']<today) &
            (changes_df['Date']>'1999-01-01')]
            .reset_index(drop=True))

    def get_all_tickers(historical_changes, ticker_df):
        """Returns a list of all tickers currently in the sp500 
        and and all tickers that have been removed from the 
        sp500 since 1999-01-01"""
        all_tickers = ticker_df['Symbol'].to_list() + \
            historical_changes['Removed'].dropna().to_list()
        all_tickers = [*set(all_tickers)]
        return all_tickers


    def create_df():
        """executes a few functions in order to return the main
        df which has market close data since 1999 for all 
        tickers currently in sp500 and all tickers that have 
        been removed from sp500 since 1999-01-01. Also drops 
        all tickers that had only nan results.  
        Puts the resulting dataframe in the database."""
        engine = sqlalchemy.create_engine(
            'sqlite:///backtesting.db')
        with engine.connect() as con:
            changes_df = pd.read_sql('changes_df', con=con)
            ticker_df = pd.read_sql('ticker_df', con=con)
        historical_changes = get_historical_changes(changes_df)
        all_tickers = get_all_tickers(
            historical_changes, ticker_df)
        df = yf.download(
            all_tickers, start='1999-01-01')['Adj Close']
        return (df.drop(columns=
            [df[x].name for x in df.columns if df[x].count()==0]))

    marketCloseData = create_df()
    engine = sqlalchemy.create_engine(
        'sqlite:///backtesting.db')
    with engine.connect() as con:
        marketCloseData.to_sql('marketCloseData', con=con)

if __name__ == '__main__':
    main()