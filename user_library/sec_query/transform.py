import os, glob, re , sys, time
import polars as pl
from user_library.shared_functions import _test_mkdir, _timer

_detination_parent = os.path.join(os.getcwd(), 'sec_data', 'sec_transformed')
_unzipped_path = os.path.join(os.getcwd(), 'sec_data', 'sec_unzipped')
_txt_files = ['pre', 'sub', 'tag', 'num']
_schemas = {
    
    'pre': {
        'adsh': pl.Utf8, 
        'report': pl.Int64, 
        'line': pl.Int64, 
        'stmt': pl.Utf8, 
        'inpth': pl.Int64, 
        'rfile': pl.Utf8, 
        'tag': pl.Utf8, 
        'version': pl.Utf8, 
        'plabel': pl.Utf8, 
        'negating': pl.Int64
    },
    
    'num': {
        'adsh': pl.Utf8, 
        'tag': pl.Utf8, 
        'version': pl.Utf8, 
        'coreg': pl.Utf8, 
        'ddate': pl.Int64, 
        'qtrs': pl.Int64, 
        'uom': pl.Utf8, 
        'value': pl.Float64, 
        'footnote': pl.Utf8
    },
    
    'tag': {
        'tag': pl.Utf8, 
        'version': pl.Utf8, 
        'custom': pl.Int64, 
        'abstract': pl.Int64, 
        'datatype': pl.Utf8, 
        'iord': pl.Utf8, 
        'crdr': pl.Utf8, 
        'tlabel': pl.Utf8, 
        'doc': pl.Utf8
    },
    
    'sub':{
        'adsh': pl.Utf8, 
        'cik': pl.Int64, 
        'name': pl.Utf8, 
        'sic': pl.Int64, 
        'countryba': pl.Utf8, 
        'stprba': pl.Utf8, 
        'cityba': pl.Utf8, 
        'zipba': pl.Utf8, 
        'bas1': pl.Utf8, 
        'bas2': pl.Utf8, 
        'baph': pl.Utf8, 
        'countryma': pl.Utf8, 
        'stprma': pl.Utf8, 
        'cityma': pl.Utf8, 
        'zipma': pl.Utf8, 
        'mas1': pl.Utf8, 
        'mas2': pl.Utf8, 
        'countryinc': pl.Utf8, 
        'stprinc': pl.Utf8, 
        'ein': pl.Int64, 
        'former': pl.Utf8, 
        'changed': pl.Int64, 
        'afs': pl.Utf8, 
        'wksi': pl.Int64, 
        'fye': pl.Int64, 
        'form': pl.Utf8, 
        'period': pl.Int64, 
        'fy': pl.Int64, 
        'fp': pl.Utf8, 
        'filed': pl.Int64, 
        'accepted': pl.Utf8, 
        'prevrpt': pl.Int64, 
        'detail': pl.Int64, 
        'instance': pl.Utf8, 
        'nciks': pl.Int64, 
        'aciks': pl.Int64
    }
}

# join files with .txt extension and write .parquet with individual filing
def _transform_archive(path, destination):
    folder = os.path.basename(path)
    txt_dict = {}
    for txt in glob.glob(path + '/*.txt'):
        txt_dict[os.path.basename(txt).split('.')[0]] = txt
    pre, sub, tag, num = (
        pl.scan_csv(
            txt_dict[txt],
            separator='\t',
            ignore_errors=True,
            dtypes=_schemas[txt]
        ) for txt in _txt_files
    )
    origin = pl.lit(folder + '.zip').alias('originFileTag')
    try:
        df = num.join(sub, on='adsh', how='inner')\
        .join(pre, on=['adsh', 'version', 'tag'], how='inner')\
            .join(tag, on=['tag', 'version'], how='inner')
        df = df.with_columns(
            pl.col(
                ['ddate', 'changed', 'period', 'filed']
            ).cast(pl.Utf8).str.to_datetime(
                format='%Y%m%d',
                time_unit='ms'
            ),
            pl.col('accepted').str.to_datetime(
                format='%Y-%m-%d %H:%M:%S.0',
                time_unit='ms'
            ),
            origin
        )
        for form in sub.select(pl.col('form')).unique().collect().rows():
            form = form[0]
            sink_destination = os.path.join(destination, form.replace('/', '-') + '.parquet')
            df.filter(pl.col('form') == form).sink_parquet(sink_destination)
        return 'complete'
    except Exception as e:
        return f'{e} from {path}'
    
# run transformation on all files in dir, currently only takes a dir as path input and not
# individual files. Code can easily be extended to handle single files
def transform(path=None, destination=None, rmtxt=False):
    start = time.time()
    
    path = _unzipped_path if path is None else _test_mkdir(path)
    destination = _detination_parent if destination is None else _test_mkdir(destination)
    try:
        prev_transformations = os.listdir(destination)
    except:
        prev_transformations = []
    zip_re = re.compile(r'\d{4}q[1-4]')
    print('\nTransforming files in %s...\n' % path, flush=True)
    for folder in sorted(os.listdir(path)):
        if zip_re.match(folder):
            if folder not in prev_transformations:
                sys.stdout.write('  %s... ' % folder)
                sys.stdout.flush()
                filename_path = os.path.join(path, folder)
                status = _transform_archive(filename_path, _test_mkdir(os.path.join(destination, folder)))
                print(status, flush=True)
            else:
                sys.stdout.write('  %s... ' % folder)
                sys.stdout.flush()
                print('already transformed', flush=True)
    if rmtxt:
        try:
            os.rmdir(path)
        except:
            pass
        
    end = time.time()
    
    _timer(start, end)