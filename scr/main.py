from datetime import datetime

from control_dates import get_last_time
from get_data import get_data_uf
from manage_files import update_file
from time_format import hours_to_timestamp, timestamp_to_date, date_to_timestamp
from interfaces.repository import clone_repository, commit_and_push
from SETUP import MIN_DATE
from treat_data import detect_missing, detect_wrong_date, separate_by_date

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
        if len(to_save[data_name]) == 0:
            continue
        r = separate_by_date(to_save[data_name])
        for date in r:
            data = r[date]
            if data_name == 'data':
                update_file(uf, date, data)
            else:
                update_file(uf, date, data, data_name)
            print('Saved', data_name, date, uf)

def select_dates(uf, update_all=False):
    if update_all:
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


def update_data(request):
    uf_list = request['uf_list']
    update_all = request['update_all']
    commit_msg = request['commit_msg']
    for uf in uf_list:
        for a, b in select_dates(uf, update_all):
            print('\nDATE: ', a, b, timestamp_to_date(a), '-', timestamp_to_date(b))
            update_for_dates(a, b, uf)
        commit_and_push(commit_msg)
    return 'done'


if __name__ == '__main__':
    request = dict()
    request['uf_list'] = ['SP']
    request['update_all'] = True
    request['commit_msg'] = 'Test update.'
    update_data(request)



