from user_library.shared_functions import _request, _save_df
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from requests import get, exceptions
from time import sleep
import pandas as pd
import re

_elements = [
    ('section', {'class':'live-story-post__body'}),
    ('div', {'class':'sc-bdVaJa post-content-rendered render-stellar-contentstyles__Content-sc-9v7nwy-0 erzhuK'})
]
_no_articles_str = "It could be you, or it could be us, but there's no page here."

def _get_date_range(start, end):
    
    # error handling
    try:
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')
    except Exception as e:
        raise SystemExit(e)
    
    # run loop to retrive dates
    current_date = start
    dates = []
    while current_date <= end:
        dates.append(current_date.strftime('%m-%d-%y'))
        current_date += timedelta(days=1)
    
    return dates

def _request(url):
    
    # error handling relating to url connection
    try:
        response = get(url)
    except exceptions.RequestException as e:
        raise SystemExit(e)
    
    return response

def _scrape(date):
    
    # base url to cnn website
    url = f'https://edition.cnn.com/world/live-news/coronavirus-pandemic-{date}-intl/index.html'
    
    # get repsonse
    html = _request(url)
    
    return BeautifulSoup(html.content, features="html.parser")

def _get_headlines(soup_obj):
    
    # iterate over headlines in soup object
    headlines = {}
    for i, headline in enumerate(soup_obj.findAll('h2')):
        headlines[i] = headline.text
        
    # no articles present and header handling
    if _no_articles_str in headlines.values():
        headlines = {}
    else:
        vals = list(headlines.values())[2:]
        headlines = {k: v for k, v in enumerate(vals)}
        
    return headlines

def _get_body(soub_obj):
    
    # initialize dict to store results
    body_text = {}
    
    # iterate over elements to search for articles
    for element in _elements:
        # iterate over body parts in soup object
        for i, body in enumerate(soub_obj.findAll(element[0], element[1])):
            body_text[i] = body.text.strip()
        if body_text:
            break
    
    # header handling  
    vals = list(body_text.values())[2:]
    body_text = {k: v for k, v in enumerate(vals)}
        
    return body_text

def _make_raw_df(start, end):
    
    # get dates in range
    dates = _get_date_range(start, end)
    
    # iterate through dates and scrape
    list_of_df = []
    for date in dates:
        soup = _scrape(date)
        headlines = _get_headlines(soup)
        
        # break loop if headline is empty
        if not headlines:
            bodies = [None]
            headlines = [None]
            date_ls = [datetime.strptime(date, '%m-%d-%y')]
        else:
            bodies = list(_get_body(soup).values())
            headlines = list(headlines.values())
            date_ls = [datetime.strptime(date, '%m-%d-%y')] * len(bodies)
        
        # append to df
        list_of_df.append(pd.DataFrame({'Date':date_ls, 'Headline':headlines, 'Body':bodies}))
        
        # sleep 1s in between iter
        sleep(1)
    
    # concat df's    
    df = pd.concat(list_of_df)
        
    return df

def scrapeCNN(start, end, max_articles=None):
    
    # get raw df
    raw_df = _make_raw_df(start, end)
    
    # clean body text
    truncated_df = raw_df.groupby('Date').head(max_articles).reset_index(drop=True)
    bodies = truncated_df.Body
    clean_bodies = []
    for body in bodies:
        if not body:
            clean_bodies.append(body)
        else:
            body = body.replace('\n', '').replace('\xa0', ' ').replace('\t', ' ')
            body = re.sub(' +', ' ', body)
            body = re.sub('^CNN\s', '', body)
            clean_bodies.append(body)
    truncated_df['Body'] = clean_bodies
    
    # save
    _save_df(truncated_df, 'cnn_data', 'raw.parquet')