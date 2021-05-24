# This script exemplifies how to automatically get data.

from datetime import datetime, timedelta

import pandas as pd


def datetime_to_str(date):
    return date.strftime('%Y-%m-%d')


def str_to_datetime(s):
    return datetime.strptime(s, '%Y-%m-%d')


def get_next_day(date):
    return date + timedelta(days=1)


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
    i = date_from
    while i <= date_until:
        date_str = datetime_to_str(i)  # Get the date in the same format as the repository files
        uri = get_uri(state, directory, date_str)
        print(date_str)
        df_n = pd.read_csv(uri)
        df = df.append(df_n)
        i = get_next_day(i)

print(df)
