
# python file to populate backtesting.db database.  If database has not been
# created previously or has been deleted, this will populate everything new.
# If the dataframe already exists, all the dataframe will be overwritten

# After running this script, all dataframes should be created and 
# the jupyter notebook "evaluate_strategy can be run to calculate 
# the performance of the strategy"

from preprocessing import wiki, create_marketClose
import create_monthly_calcs
import create_return_dataframes

def main():
    wiki.main()
    create_marketClose.main()
    create_monthly_calcs.main()
    create_return_dataframes.main()

if __name__ == "__main__":
    main()

