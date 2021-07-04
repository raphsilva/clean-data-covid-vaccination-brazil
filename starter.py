import threading
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from time import time

import pandas as pd

date_from = '2021-04-04'
date_until = '2021-06-06'
uf_list = ['SP', 'MG', 'MS']

time_i = time()
lock = threading.Lock()


def datetime_to_str(date):
    return date.strftime('%Y-%m-%d')


def str_to_datetime(s):
    return datetime.strptime(s, '%Y-%m-%d')


def get_next_day(date):
    return date + timedelta(days=1)


def sum_days(date, a):
    return date + timedelta(days=a)


def is_index(date):  # Data is saved in files separated by week. The name of the file is the Monday of the related week.
    if date.weekday() == 0:
        return True
    return False


def get_uri(state, directory, date):
    return f'https://raw.githubusercontent.com/raphsilva/data-covid-vaccination-brazil/master/data/{state}/{directory}/{date}.csv'


# Convert to datetime to make it easier to iterate
date_from = str_to_datetime(date_from)
date_until = str_to_datetime(date_until)

data = pd.DataFrame()


def get_data_uf_directory(uf, directory, date):
    global data
    date_str = datetime_to_str(date)  # Get the date in the same format as the repository files
    uri = get_uri(uf, directory, date_str)
    with lock:
        print('    Getting data: ', uf, date_str, directory)
    try:
        df_n = pd.read_csv(uri)
        df_n['data'] = date
        df_n['directory'] = directory
        df_n['uf'] = uf
        if 'paciente_id' in df_n:
            del df_n['paciente_id']
    except Exception as e:
        print('  ERROR', e)
    with lock:
        data = data.append(df_n)


pool = ThreadPool(processes=10)
futures = list()
for uf in uf_list:
    for directory in ['consistent', 'inconsistent']:
        date_from = sum_days(date_from, -date_from.weekday())  # Get the Monday before
        date = date_from
        while date <= date_until:
            if is_index(date):
                future = pool.apply_async(get_data_uf_directory, (uf, directory, date))
                futures.append(future)
            date = get_next_day(date)

for f in futures:
    f.get()  # Wait for all the processes to finish.

print(data)

print(f'Finished in {int(time() - time_i)} seconds.')
