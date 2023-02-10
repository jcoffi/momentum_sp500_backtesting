import pandas as pd
import sqlalchemy
import numpy as np

def main():

    engine = sqlalchemy.create_engine('sqlite:///backtesting.db')
        
    with engine.connect() as con:
        df = pd.read_sql_table(
            "marketCloseData", con=con, index_col='Date')

    # create mtl dataframe which is monthly returns for each
    # ticker
    mtl = (df.pct_change()+1)[1:].resample('M').prod()

    def get_rolling_ret(df, n):
        """Create dataframe displaying rolling returns for 
        each ticker for n months with date as the index"""
        return df.rolling(n).apply(np.prod)

    ret_12, ret_6, ret_3 = get_rolling_ret(mtl, 12), \
        get_rolling_ret(mtl, 6), get_rolling_ret(mtl, 3)

    with engine.connect() as con:
        mtl.to_sql("monthlyReturns", con=con, if_exists="replace")
        ret_12.to_sql("12MonthReturns", con=con, if_exists="replace")
        ret_6.to_sql("6MonthReturns", con=con, if_exists="replace")
        ret_3.to_sql("3MonthReturns", con=con, if_exists="replace")

if __name__ == '__main__':
    main()