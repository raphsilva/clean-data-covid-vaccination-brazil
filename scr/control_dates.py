import json
import os

from SETUP import MIN_DATE

from time_format import date_to_timestamp

from manage_files import get_directory_path


def get_last_time(uf):
    path = get_directory_path(uf)
    if not os.path.isdir(path) or len(os.listdir(path)) == 0:
        return date_to_timestamp(MIN_DATE)
    last_file = sorted(os.listdir(path))[-1]
    last_date = last_file.split('.')[0]
    return date_to_timestamp(last_date)