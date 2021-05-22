from datetime import datetime

from control_dates import get_last_time
from get_data import get_data_uf
from interfaces.repository import clone_repository, commit_and_push
from manage_files import update_file_uf_date, update_info_updates
from time_format import hours_to_timestamp, timestamp_to_date, date_to_timestamp
from treat_data import detect_missing, detect_wrong_date, separate_by_date, aggregate_count
from multiprocessing.pool import ThreadPool
from time import time

DATA_SIZE_DAYS = 4
OVERLAP_DAYS = 2
RECENT_DAYS = 7
MAX_DAYS = 14

# update local repository
print('Cloning repository.')
clone_repository()
print('Done: cloned repository.')

date_now = datetime.utcnow().timestamp() * 1000


def update_for_dates(date_A, date_B, uf):
    print(f'GETTING {uf} {timestamp_to_date(date_A)}')
    t0 = time()
    data = get_data_uf(uf, date_A, date_B)
    spent = time()-t0

    if len(data) == 0:
        print('No data')
        return

    total_data_len = len(data)

    missing, complete = detect_missing(data)
    complete = aggregate_count(complete)
    wrong_date, complete = detect_wrong_date(complete)

    to_save = dict()
    if len(missing) > 0:
        to_save['missing_demography'] = missing
    if len(wrong_date) > 0:
        to_save['wrong_date'] = wrong_date
    if len(complete) > 0:
        to_save['complete'] = complete

    for data_name in to_save:
        r = separate_by_date(to_save[data_name])
        for date in r:
            data = r[date]
            update_file_uf_date(uf, date, data, data_name)
            avg_spent_time = int(spent*data['contagem'].sum()/total_data_len)
            update_info_updates(uf, date, data, data_name, avg_spent_time)
            print('Saved', data_name, date, uf)


def decide_update_mode(uf):
    last = get_last_time(uf)
    if last is None:
        return 'beginning'
    elif date_now - last < hours_to_timestamp(4 * 24):
        return 'recent'
    else:
        return 'last'


def select_dates(uf, update_from, update_until):
    if update_from == 'beginning':
        yield 0, date_to_timestamp('2019-12-01')
        yield date_to_timestamp('2019-12-01'), date_to_timestamp('2020-12-01')
        yield date_to_timestamp('2020-12-01'), date_to_timestamp('2021-01-17')
        a = date_to_timestamp('2021-01-17')
    elif update_from == 'recent':
        a = date_now - hours_to_timestamp(RECENT_DAYS * 24)
    elif update_from == 'last':
        a = get_last_time(uf) - hours_to_timestamp(OVERLAP_DAYS * 24)
    else:
        a = date_to_timestamp(update_from)
    if update_until is None:
        update_until = a + hours_to_timestamp(MAX_DAYS * 24)
    else:
        update_until = date_to_timestamp(update_until)
    while a < update_until:
        b = a + hours_to_timestamp(DATA_SIZE_DAYS * 24)
        yield a, b
        a = b


def update_data(uf, dates, commit_msg):
    pool = ThreadPool(processes=4)
    futures = list()
    for a, b in dates:
        print('\nDATE: ', a, b, timestamp_to_date(a), '-', timestamp_to_date(b))
        future = pool.apply_async(update_for_dates, (a, b, uf))
        futures.append(future)
    d = 0
    for f in futures:
        f.get()
        d += 1
        print(f'done {uf} {d}/{len(dates)}')
    commit_and_push(commit_msg)


def handle_request(request):
    uf_list = request['uf_list']
    if 'update_from' not in request:
        request['update_from'] = 'auto'
    if 'update_until' not in request:
        request['update_until'] = None
    update_from = request['update_from']
    update_until = request['update_until']
    commit_msg = request['commit_msg']
    for uf in uf_list:
        if update_from == 'auto':
            update_from = decide_update_mode(uf)
            print('Update mode:', update_from)
        dates = list(select_dates(uf, update_from, update_until))
        if len(dates) == 0:
            print('Nothing to update.')
            break
        t0 = time()
        update_data(uf, dates, commit_msg)
        tt = time() - t0
        print(f'Finished {uf} in {int(tt)} seconds. -- {timestamp_to_date(dates[0][0])} - {timestamp_to_date(dates[-1][1])}')
    return 'done'


if __name__ == '__main__':
    request = dict()
    request['uf_list'] = ['SP']
    # request['update_from'] = ['beginning', 'last', 'few_last', '2021-05-12'][2]
    # request['update_until'] = '2021-04-01'
    request['commit_msg'] = '[data] Update.'
    handle_request(request)
