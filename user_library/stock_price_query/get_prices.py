import yfinance as yf
from user_library.shared_functions import _save_df

# function to query prices and return adj close prices
def get_prices(tickers, start, end):
    all_prices = yf.download(tickers, start, end, period='1d', group_by='Ticker')
    adj_close = all_prices.xs('Adj Close', level=1, axis=1).reset_index(drop=False)
    _save_df(adj_close, 'price_data', 'historical.parquet')