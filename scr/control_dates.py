import os

from SETUP import MIN_DATE
from manage_files import get_directory_path
from time_format import date_to_timestamp


def get_last_time(uf):
    path = get_directory_path(uf)
    if not os.path.isdir(path) or len(os.listdir(path)) == 0:
        return date_to_timestamp(MIN_DATE)
    last_file = sorted([f for f in os.listdir(path) if os.path.isfile(f'{path}/{f}')])[-1]
    last_date = last_file.split('.')[0]
    return date_to_timestamp(last_date)
