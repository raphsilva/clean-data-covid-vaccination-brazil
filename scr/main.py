from get_data import get_data
from datetime import datetime, timedelta, date
from manage_files import update_file

from control_dates import get_registered_time, register_time

uf = 'SP'
date = '2021-04-22'

date_A = datetime.strptime(date, '%Y-%m-%d')
date_B = date_A + timedelta(days=7)
date_B = min(date_B, datetime.now() - timedelta(hours=6)) # don't get after 6 hours
date_A = date_A.strftime('%Y-%m-%d %H:%M:%S')
date_B = date_B.strftime('%Y-%m-%d %H:%M:%S')

d1 = get_data(uf, '1', date)
d1['dose'] = '1'
d2 = get_data(uf, '2', date)
d2['dose'] = '2'

data = d1.append(d2)
data = data[['dose', 'count']]
data = data.reset_index()

g = data.groupby('date_vaccinated')

for d in g:
    dt = d[0]
    info = d[1]
    info = info[['age', 'dose', 'count']]
    info = info.sort_values(by=['dose', 'age'])
    update_file(uf, dt, info)
    print('Saved', dt)

register_time(uf, date_B)
