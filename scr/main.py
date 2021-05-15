from datetime import datetime

from control_dates import get_last_time
from get_data import get_data
from manage_files import update_file
from time_format import hours_to_timestamp, timestamp_to_date
from interfaces.repository import clone, commit_and_push

uf_list = ['SP']

# update local repository
print('Cloning repository.')
clone()
print('Done: cloned repository.')

date_now = datetime.utcnow().timestamp() * 1000

for uf in uf_list:
    date_A = get_last_time(uf) - hours_to_timestamp(7*24)
    print('DATE: ', date_A, timestamp_to_date(date_A))
    while date_A < date_now - hours_to_timestamp(6):
        date_B = date_A + hours_to_timestamp(7 * 24)
        date_B = min(date_B, date_now - hours_to_timestamp(2))  # don't get after 6 hours
        print('DATE: ', date_A, timestamp_to_date(date_A), '-', timestamp_to_date(date_B))

        d1 = get_data(uf, '1', date_A, date_B)
        d1['dose'] = '1'
        d2 = get_data(uf, '2', date_A, date_B)
        d2['dose'] = '2'
        date_A = date_B

        data = d1.append(d2)

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

commit_and_push("Updating data.")