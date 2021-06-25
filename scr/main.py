from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from time import time
import random

import pandas as pd

from control_dates import get_last_time
from get_data import get_data_uf
from interfaces.repository import clone_repository, commit_and_push
from manage_files import update_file_uf_date, update_info_updates, read_info_updates
from time_format import hours_to_timestamp, timestamp_to_date, date_to_timestamp, days_since, dates_range, datetime_to_str
from treat_data import detect_missing, detect_wrong_date, separate_by_date, aggregate_count

DATA_SIZE_DAYS = 4
OVERLAP_DAYS = 4
RECENT_DAYS = 21
MIN_DAYS = 7
MAX_DAYS = 28

LOCAL_TEST = False

# update local repository
if not LOCAL_TEST:
    print('Cloning repository.')
    clone_repository()
    print('Done: cloned repository.')

date_now = int(datetime.utcnow().timestamp() * 1000)


def update_for_dates(date_A, date_B, uf):
    print(f'GETTING {uf} {timestamp_to_date(date_A)}')
    t0 = time()
    data = get_data_uf(uf, date_A, date_B)
    spent = time() - t0

    print(date_A, date_B, uf)
    print(data)
    print(len(data))
    exit()

    if len(data) == 0:
        print('No data')
        return

    total_data_len = len(data)

    missing, complete = detect_missing(data)
    complete = aggregate_count(complete)
    wrong_date, complete = detect_wrong_date(complete)

    to_save = dict()
    if len(missing) > 0:
        to_save['inconsistent'] = missing
    if len(wrong_date) > 0:
        to_save['wrong_date'] = wrong_date
    if len(complete) > 0:
        to_save['consistent'] = complete

    for data_name in to_save:
        r = separate_by_date(to_save[data_name])
        for date in r:
            data = r[date]
            update_file_uf_date(uf, date, data, data_name)
            avg_spent_time = round(spent * data['contagem'].sum() / total_data_len)
            update_info_updates(uf, date, data, data_name, avg_spent_time)
            print('Saved', data_name, date, uf)


def decide_update_mode(uf):
    last = get_last_time(uf)
    if last is None:
        return 'beginning'
    else:
        return 'smart'
    # elif date_now - last < hours_to_timestamp(4 * 24):
    #     return 'recent'
    # else:
    #     return 'last'


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
        b = a + hours_to_timestamp(DATA_SIZE_DAYS * 24) # TODO format keeps changing
        yield a, b
        a = b


def dates_range_s(dates, size):
    r = list()
    for d in dates:
        a = date_to_timestamp(datetime_to_str(d))  # TODO format keeps changing
        b = date_to_timestamp(datetime_to_str(d+timedelta(days=size)))
        r.append((a, b))
    return r


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
        if LOCAL_TEST:
            print('Not commiting.')
        else:
            commit_and_push(commit_msg)
        print(f'done {uf} {d}/{len(dates)}')




def select_dates_smart(uf):
    max_score = 10000
    data = read_info_updates(uf)
    data = data.drop_duplicates(['data_aplicaçao'])
    data = data[['data_aplicaçao', 'data_atualizacao']]
    data['data_aplicaçao'] = pd.to_datetime(data['data_aplicaçao'], format='%Y-%m-%d')
    data['data_atualizacao'] = pd.to_datetime(data['data_atualizacao'], format='%Y-%m-%d')
    fill_dates = dates_range(max(data['data_aplicaçao']), datetime.today())
    data = pd.merge(data, pd.Series(fill_dates, name='data_aplicaçao'), on='data_aplicaçao', how='outer')
    data['since_aplicaçao'] = data['data_aplicaçao'].apply(lambda a: days_since(a))
    data['since_atualizacao'] = data['data_atualizacao'].apply(lambda a: days_since(a))
    data['score'] = data['since_atualizacao'] - data['since_aplicaçao']/5
    data['score'] = data['score'].fillna(max_score)
    data_c = data[data['score']>0]
    if len(data_c) < MIN_DAYS:
        data_c = data
    data = data_c
    data = data.sort_values(by=['score', 'data_aplicaçao'], ascending=[False, True])
    never_updated_count = data['score'].value_counts()[max_score]
    cut = MIN_DAYS
    if never_updated_count > MIN_DAYS:
        cut = MAX_DAYS
    data = data[:cut]
    dates = list(data['data_aplicaçao'])
    return dates_range_s(dates, 1)


def handle_request(request):
    if isinstance(request, dict):
        request_json = request
    else:
        request_json = request.get_json(silent=True)
    uf_list = request_json['uf_list']
    if 'update_from' not in request_json:
        request_json['update_from'] = 'auto'
    if 'update_until' not in request_json:
        request_json['update_until'] = None
    update_from = request_json['update_from']
    update_until = request_json['update_until']
    if 'commit_msg' not in request_json:
        commit_msg = '[data] Update.'
    else:
        commit_msg = request_json['commit_msg']

    random.shuffle(uf_list)
    print('UFs:', uf_list)

    for uf in uf_list:
        if update_from == 'auto':
            update_from = decide_update_mode(uf)
            print('Update mode:', update_from)

        if update_from == 'smart':
            dates = select_dates_smart(uf)
        else:
            dates = list(select_dates(uf, update_from, update_until))

        if len(dates) == 0:
            print('Nothing to update.')
            break

        t0 = time()
        update_data(uf, dates, commit_msg)
        tt = time() - t0

        print(f'Finished {uf} in {int(tt)} seconds. -- {timestamp_to_date(dates[0][0])} - {timestamp_to_date(dates[-1][1])}')

    return f'finished in {int(tt)} seconds -- {uf_list} -- {dates}'


if __name__ == '__main__':
    request = dict()
    request['uf_list'] = ['MS']
    # request['update_from'] = ['beginning', 'last', 'few_last', '2021-05-12'][2]
    # request['update_until'] = '2021-04-01'
    request['update_from'] = 'smart'
    request['commit_msg'] = '[data] Update.'
    handle_request(request)
