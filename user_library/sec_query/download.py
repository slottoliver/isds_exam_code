import os, datetime, sys, re
from zipfile import ZipFile
from user_library.shared_functions import _test_mkdir, _request, _qtr_range

_baseurl = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets'
_download_dir = os.path.join(os.getcwd(), 'sec_data', 'sec_zip')
_extraction_dir = os.path.join(os.getcwd(), 'sec_data', 'sec_unzipped')

# establish connection to url and execute download
def _download_archive(qtr, path):
    path = _test_mkdir(path)
    filename = qtr + '.zip'
    url = '%s/%s' % (_baseurl, filename)
    with _request(url) as response:
        if response.status_code == 200:
            with open(os.path.join(path, filename), 'wb') as file:
                file.write(response.content)
                return 'complete'
        else:
            return response

# unzip files and store        
def _extract_archive(path, destination, rmzip):
    zip_filename = os.path.split(path)[1]
    qtr = zip_filename.replace('.zip','')
    destination = os.path.join(destination, qtr)
    with ZipFile(path) as zip_file:
        zip_file.extractall(destination)
    if rmzip:
        os.remove(path)

# get latest qtr if end not specified        
def _latest_qtr():
    today = datetime.date.today()
    yr, qtr = today.year, (today.month-1)//3
    yr, qtr = (yr-1, 4) if qtr==0 else (yr, qtr)
    qtr = '%sq%s' % (yr, qtr)
    with _request('%s/%s.zip' % (_baseurl, qtr)) as response:
        if response.status_code == 200:
            return qtr
        else:
            yr, qtr = (yr-1, 4) if qtr==1 else (yr, qtr-1)
            return '%sq%s' % (yr, qtr)
 
# execute download for every date in qtr range        
def download(startqtr='2009q1', endqtr=None, path=None):
    path = _download_dir if path is None else _test_mkdir(path)
    try:
        files_alreay_in_path = os.listdir(path)
    except:
        files_alreay_in_path = []
    sys.stdout.write('\nDownloading %s/' % _baseurl)
    sys.stdout.flush()    
    print('...\n')
    endqtr = _latest_qtr() if endqtr is None else endqtr
    for qtr in _qtr_range(startqtr, endqtr):
        sys.stdout.write('  %s.zip... ' % qtr)
        sys.stdout.flush()
        if not qtr + '.zip' in files_alreay_in_path:
            status = _download_archive(qtr, path)
            print(status, flush=True)
        else:
            print('already downloaded!', flush=True)

# unzip every file in dir related to the data. Currently only takes a dir as path input and not
# individual files. Code can easily be extended to handle single files           
def extract(path=None, destination=None, rmzip=False):
    path = _download_dir if path is None else _test_mkdir(path)
    destination = _extraction_dir if destination is None else _test_mkdir(destination)
    zip_re = re.compile(r'\d{4}q[1-4]\.zip')
    print('\nExtracting %s...\n' % path, flush=True)
    for zip in sorted(os.listdir(path)):
        if zip_re.match(zip):
            sys.stdout.write('  %s... ' % zip)
            sys.stdout.flush()
            zip_path = os.path.join(path, zip)
            _extract_archive(zip_path, destination, rmzip)
            print('complete', flush=True)
    if rmzip:
        try:
            os.rmdir(path)
        except:
            pass