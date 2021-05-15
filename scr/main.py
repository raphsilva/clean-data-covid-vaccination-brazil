from datetime import datetime

from control_dates import get_last_time
from get_data import get_data
from manage_files import update_file
from time_format import hours_to_timestamp, timestamp_to_date, date_to_timestamp
from interfaces.repository import clone, commit_and_push
from SETUP import MIN_DATE
from treat_data import detect_missing, detect_wrong_date, separate_by_date

uf_list = ['SP']

# update local repository
print('Cloning repository.')
clone()
print('Done: cloned repository.')

date_now = datetime.utcnow().timestamp() * 1000

for uf in uf_list:
    date_A = get_last_time(uf) - hours_to_timestamp(7*24)
    date_A = max(date_A, date_to_timestamp(MIN_DATE))
    print('DATE: ', date_A, timestamp_to_date(date_A))
    while date_A < date_now - hours_to_timestamp(6):
        date_B = date_A + hours_to_timestamp(7 * 24)
        date_B = min(date_B, int(date_now - hours_to_timestamp(2)))  # don't get after 6 hours
        print('DATE: ', date_A, date_B, timestamp_to_date(date_A), '-', timestamp_to_date(date_B))

        d1 = get_data(uf, '1', date_A, date_B)
        d1['dose'] = '1'
        d2 = get_data(uf, '2', date_A, date_B)
        d2['dose'] = '2'
        date_A = date_B

        data = d1.append(d2)

        print('LENGTH', len(data))

        if len(data) == 0:
            continue

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
                print('Saved', date, uf)


commit_and_push("Updating data.")