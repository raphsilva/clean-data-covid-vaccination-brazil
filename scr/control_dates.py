import os
import json
from datetime import datetime

FILEPATH = 'register_dates.json'
from time_format import date_to_timestamp, hours_to_timestamp


def read_register():
    if os.path.isfile(FILEPATH):
        return json.loads(open(FILEPATH).read())
    else:
        return dict()


def get_registered_time(uf):
    register = read_register()
    if uf in register:
        r = register[uf]
    else:
        r = date_to_timestamp('2021-01-15')
    return r


def register_time(uf, time):
    if os.path.isfile(FILEPATH):
        register = read_register()
    else:
        register = dict()
    register[uf] = time
    f = open(FILEPATH, 'w')
    f.write(json.dumps(register))
