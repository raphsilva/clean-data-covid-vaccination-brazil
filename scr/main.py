from get_data import get_data
from datetime import datetime, timedelta, date

from control_dates import get_registered_time, register_time

uf = 'SP'
date = '2021-04-22'

date_A = datetime.strptime(date, '%Y-%M-%d')
date_B = date_A + timedelta(days=7)

d1 = get_data(uf, '1', date)
d1['dose'] = '1'
d2 = get_data(uf, '2', date)
d2['dose'] = '2'

data = d1.append(d2)
data = data[['dose', 'count']]

register_time(uf, date_B)

print(data)