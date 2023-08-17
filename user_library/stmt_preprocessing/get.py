import polars as pl
import pandas as pd
from user_library.shared_functions import _lookup_cik

class GetFinancialStatements():
    
    def __init__(self, path):
        self.path = path
        self.scanner = self._scan()
        self.clean_df = self.clean()
            
    def _scan(self):
        df = pl.scan_parquet(self.path)
        return df
    
    def _filter(self):
        df = self.scanner
        df = df.with_columns(
            pl.col('fp').str.extract(r'Q(\d+)').cast(pl.Int64).alias('qtr_helper')
        )
        _filters = (
            (pl.col('coreg') == None) & 
            (
                ((pl.col('form') == '10-K') & (pl.col('stmt').is_in(['EQ', 'CF', 'IS'])) & (pl.col('qtrs') == 4)) | 
                ((pl.col('qtrs') == 0) & (pl.col('stmt') == 'BS')) | 
                ((pl.col('qtrs') == pl.col('qtr_helper')) & (pl.col('form') == '10-Q') & (pl.col('stmt').is_in(['IS', 'CF', 'EQ'])))
            )
        )
        df = df.filter(_filters)
        return df.collect()
    
    def clean(self):
        return self._filter()
    
    def df(self, ticker_or_cik):
        cik = _lookup_cik(ticker_or_cik)
        df = self.clean_df.filter(pl.col('cik') == cik)
        return df
    
    def company(self, ticker_or_cik):
        cik = _lookup_cik(ticker_or_cik)
        df = self.clean_df.filter(pl.col('cik') == cik)
        filters = lambda x: df.filter(pl.col('stmt') == x).pivot(
            values='value', 
            index='tag', 
            columns='ddate', 
            aggregate_function='first'
        )
        self.show = df
        self.BS = filters('BS')
        self.IS = filters('IS')
        self.CF = filters('CF')
        self.EQ = filters('EQ')
        #self.All = pl.concat([self.BS, self.IS, self.CF, self.EQ]).unique()
        
        return self
   
def stmt_performance(tickers, data):
    dfs_idx = {}
    for ticker in tickers:
        comp = data.company(ticker)

        de_ratio = (comp.BS.filter(pl.col('tag') == 'Liabilities').drop(columns='tag') \
            / comp.BS.filter(pl.col('tag') == 'Assets').drop(columns='tag')).transpose().to_series().to_list()
            
        cm_ratio = (comp.IS.filter(pl.col('tag') == 'OperatingIncomeLoss').drop(columns='tag') \
            / comp.IS.filter(pl.col('tag') == 'Revenues').drop(columns='tag')).transpose().to_series().to_list()
            
        ocf = comp.CF.filter(pl.col('tag') == 'NetCashProvidedByUsedInOperatingActivities').drop(columns='tag').transpose().to_series().to_list()

        dt_ls = comp.BS.columns
        dt_ls.remove('tag')

        years = []
        quarters = []
        for date_str in dt_ls:
            year = date_str[:4]
            quarter = (int(date_str[5:7]) - 1) // 3 + 1
            years.append(int(year))
            quarters.append(int(quarter))

        df = pd.DataFrame(data={'Ticker':[ticker]*len(dt_ls), 'Year':years, 'Quarter':quarters, 'D/E, Ratio':de_ratio, 'Contrib. Mgn., Ratio':cm_ratio, 'OFC':ocf})
        mask = (df['Year'] > 2019) | ((df['Year'] == 2019) & (df['Quarter'] >= 3))
        mask &= (df['Year'] < 2021) | ((df['Year'] == 2021) & (df['Quarter'] <= 2))
        df = df[mask]
        dfs_idx[ticker] = df
        
    df = pd.concat(list(dfs_idx.values())).pivot_table(index=['Year', 'Quarter'], columns='Ticker')
    return df