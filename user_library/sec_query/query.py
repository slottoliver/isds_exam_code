import glob, os, sys, time, shutil
import polars as pl
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from user_library.shared_functions import _qtr_range, _lookup_cik, _timer, _test_mkdir

_transformed_dir = os.path.join(os.getcwd(), 'sec_data', 'sec_transformed')
_query_destination_temp = os.path.join(os.getcwd(), 'queries_temp')
_query_destination = os.path.join(os.getcwd(), 'sec_data', 'queries')

# function to get the available table names in the transformed folder
def _get_table_names(path):
    files = {}
    for file in glob.glob(path + '/**/*.parquet'):
        base = os.path.basename(file).strip('.parquet')
        parent = os.path.basename(os.path.dirname(file))
        if parent in files:
            files[parent].append(base)
        else:
            files[parent] = [base]
    return files

# get relevant rows from filing and dump in new dest
def _query_companies(path, destination, companies):
    scanner = pl.scan_parquet(path)
    df = scanner.filter(
        (pl.col('cik').is_in(companies)) & (pl.col('ddate') == pl.col('period'))
    )
    df.sink_parquet(destination)
    return 'complete'

# function to run queries on specified range
def _query_filings(startqtr, endqtr, filings, companies, path):
    filings_in_path_dict = _get_table_names(path)
    periods_in_path_list = list(filings_in_path_dict.keys())
    query_periods = _qtr_range(startqtr, endqtr)
    union_periods = set(periods_in_path_list) & set(query_periods)
    difference_periods = set(query_periods) ^ union_periods
    if difference_periods:
        sys.stdout.write(f"Data from periods: {', '.join(list(difference_periods))}, is not available.\n\n")
    sys.stdout.write(f"Extracting data from: {', '.join(list(union_periods))}\n\n")
    for period in union_periods:
        for filing in filings:
            filing_path = f'{path}/{period}/{filing}.parquet'
            temp_query_name = period + '_' + filing + '.parquet'
            temp_sink_path = os.path.join(_query_destination_temp, temp_query_name)
            sys.stdout.write(f'   Query filing {filing} for {period}... ')
            sys.stdout.flush()
            if not filing in filings_in_path_dict[period]:
                print(f'{filing} not available for {period}', flush=True)
            else:
                _test_mkdir(_query_destination_temp)
                status = _query_companies(filing_path, temp_sink_path, companies)
                print(status, flush=True)
    return 'complete'

def _merge(destination):
    dataset = ds.dataset(_query_destination_temp, format='parquet')
    scanner = dataset.scanner()
    pq.write_table(scanner.to_table(), destination)
    shutil.rmtree(_query_destination_temp)
    return 'complete'

def query(startqtr, endqtr, filings, companies, path=None, destination=None):
    start = time.time()
    
    companies = _lookup_cik(companies)
    if type(filings) is not list:
        filings = [filings]
    path = _transformed_dir if path is None else _test_mkdir(path)
    destination = _test_mkdir(_query_destination) if destination is None else _test_mkdir(destination)
    sys.stdout.write(f'Quering companies: {", ".join([str(x) for x in list(companies)])}\n\n')
    status_query = _query_filings(startqtr, endqtr, filings, companies, path)
    sys.stdout.write('\nQuery is ')
    sys.stdout.flush()
    print(status_query, flush=True)
    sys.stdout.write(f'\nStarting to merge files in {_query_destination_temp}\n\n')
    sys.stdout.write(f'   merging... ')
    sys.stdout.flush()
    status_merge = _merge(os.path.join(destination, 'merged.parquet'))
    print(status_merge, flush=True)

    end = time.time()
    _timer(start, end)
    
    
    
    
            
    

