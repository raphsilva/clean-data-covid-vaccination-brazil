from get_data import get_data
from datetime import datetime, timedelta, date
from manage_files import update_file

from control_dates import get_registered_time, register_time

from time_format import date_to_timestamp, hours_to_timestamp, timestamp_to_date

uf_list = ['SP']

for uf in uf_list:
    date_A = get_registered_time(uf)
    print('DATE: ', date_A, timestamp_to_date(date_A))
    while date_A < date_to_timestamp('2021-05-01'):
        date_A = get_registered_time(uf)
        date_B = date_A + hours_to_timestamp(7*24)
        date_B = min(date_B, datetime.utcnow().timestamp()*1000 - hours_to_timestamp(6)) # don't get after 6 hours
        print('DATE: ', date_A, timestamp_to_date(date_A), '-', timestamp_to_date(date_B))

        d1 = get_data(uf, '1', date_A, date_B)
        d1['dose'] = '1'
        d2 = get_data(uf, '2', date_A, date_B)
        d2['dose'] = '2'

        data = d1.append(d2)

        register_time(uf, date_B)

        print('LENGTH', len(data))

        if len(data) == 0:
            continue

        data = data[['dose', 'count']]
        data = data.reset_index()

        g = data.groupby('date_vaccinated')

        for d in g:
            dt = d[0]
            info = d[1]
            info = info[['age', 'dose', 'count']]
            info = info.sort_values(by=['dose', 'age'])
            update_file(uf, dt, info)
            print('Saved', dt, uf)

