from requests import get, exceptions
import pandas as pd
import os

# function to make url request 
def _request(url):
    try:
        return get(url)
    except exceptions.RequestException as e:
        raise SystemExit(e)

# function to control and create new directory    
def _test_mkdir(path):
    if not os.path.isabs(path):
        path = os.path.realpath(os.path.join(os.getcwd(), path))
    if not os.path.exists(path):
        os.makedirs(path)
    return path

# function to time functions
def _timer(start, end):
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print(
        '\n Execution time: {:0>2}:{:0>2}:{:05.2f}\n'.format(
            int(hours), 
            int(minutes), 
            seconds
        )
    )
    
# function to lookup ticker's cik number
def _lookup_cik(company):
    meta = pd.read_csv('./user_library/_tickers.csv', converters={
        'title':str.lower, 
        'ticker':str.lower, 
        'cik_str':int
    })
    if type(company) is not list:
        company = list([company])
    try:
        ciks = []
        for item in company:
            try:
                cik = int(item)
            except:
                lookup_ticker = meta.index[meta['ticker'].str.lower() == item].to_list()
                cik = int(meta['cik_str'].iloc[lookup_ticker].iloc[0])
            ciks.append(cik)
        return ciks
    except:
        raise IndexError('could not find cik, company not in data') from None
    
# get range between input dates
def _qtr_range(startqtr, endqtr):
    yr, endyr = int(startqtr[0:4]), int(endqtr[0:4])
    files = ['%sq%s' % (yr, qtr) for yr in range(yr, endyr+1) for qtr in range(1,5)]
    return files[files.index(startqtr) : files.index(endqtr) + 1]

# function to save df as parquet in data_for_analysis
def _save_df(df, folder, name):
    _test_mkdir(os.path.join(os.getcwd(), folder))
    df.to_parquet(os.path.join(folder, name))
    