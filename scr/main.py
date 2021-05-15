from datetime import datetime

from control_dates import get_last_time
from get_data import get_data_uf
from manage_files import update_file
from time_format import hours_to_timestamp, timestamp_to_date, date_to_timestamp
from interfaces.repository import clone_repository, commit_and_push
from SETUP import MIN_DATE
from treat_data import detect_missing, detect_wrong_date, separate_by_date

uf_list = ['SP']

# update local repository
print('Cloning repository.')
clone_repository()
print('Done: cloned repository.')

date_now = datetime.utcnow().timestamp() * 1000


def update_for_dates(date_A, date_B, uf):

    data = get_data_uf(uf, date_A, date_B)

    print('LENGTH', len(data))

    if len(data) == 0:
        print('No data')
        return

    missing, complete = detect_missing(data)
    wrong_date, correct = detect_wrong_date(complete)

    to_save = {'missing_demography': missing,
               'wrong_date': wrong_date,
               'data': correct}

    for data_name in to_save:
        r = separate_by_date(to_save[data_name])
        for date in r:
            data = r[date]
            if data_name == 'data':
                update_file(uf, date, data)
            else:
                update_file(uf, date, data, data_name)
            print('Saved', data_name, date, uf)

UPDATE_ALL = False
def select_dates(uf):
    if UPDATE_ALL:
        yield 0, date_to_timestamp('2019-12-01')
        yield date_to_timestamp('2019-12-01'), date_to_timestamp('2020-12-01')
        yield date_to_timestamp('2020-12-01'), date_to_timestamp('2021-01-17')
        a = date_to_timestamp('2021-01-17')
    else:
        a = get_last_time(uf) - hours_to_timestamp(7 * 24)
    while a < date_now:
        b = a + hours_to_timestamp(7 * 24)
        yield a, b
        a = b



for uf in uf_list:
    for a, b in select_dates(uf):
        print('\nDATE: ', a, b, timestamp_to_date(a), '-', timestamp_to_date(b))
        update_for_dates(a, b, uf)



commit_and_push("Updating data.")