from datetime import datetime, timedelta

import pandas as pd


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


date_from = '2021-04-04'
date_until = '2021-04-06'
state = 'SP'
directories = ['consistent', 'inconsistent']

# Convert to datetime to make it easier to iterate
date_from = str_to_datetime(date_from)
date_until = str_to_datetime(date_until)

df = pd.DataFrame()

for directory in directories:
    print('Directory:', directory)
    date_from = sum_days(date_from, -date_from.weekday())  # Get the Monday before
    cur = date_from
    while cur <= date_until:
        if is_index(cur):
            date_str = datetime_to_str(cur)  # Get the date in the same format as the repository files
            uri = get_uri(state, directory, date_str)
            print(date_str)
            try:
                df_n = pd.read_csv(uri)
                df_n['data'] = cur
                df_n['directory'] = directory
                df = df.append(df_n)
            except:
                print('not found', cur)
        cur = get_next_day(cur)

print(df)