# momentum_sp500_backtesting
Backtesting a momentum trading strategy that picks a subset of stocks to buy each month from the S&amp;P 500

Information on the trading strategy can be found in strategy.txt.

To see the results of this strategy (on incomplete and possibly inaccurate data), open evaluate_strategy.ipynb

To run the repository for yourself, you can simply run through the cells of evaluate_strategy.ipynb 
or you can run populate_backtesting.py to create all the necessary dataframes, and then run 
evaluate_strategy.ipynb.

Note: for some reason, the wikipedia article containing the table of historical changes to the sp500
has recently changed the names of the table columns a couple of times.  If populate_backtesting.py is 
not working, a good file to check would be wiki.py, and see if the dropped columns line up with the 
column titles in the actual wikipedia dataframe.

Overall, this strategy seemed to slightly outperform the sp500, but I would want to replicate this with
more accurate data before actually adopting this trading strategy.
