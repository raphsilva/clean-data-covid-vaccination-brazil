import os
import json
from datetime import datetime

FILEPATH = 'register_dates.json'


def read_register():
    return json.loads(open(FILEPATH).read())


def get_registered_time(uf):
    r = read_register()[uf]
    return datetime.strptime(r, '%Y-%m-%d %H:%M:%S')


def register_time(uf, time):
    time = str(time)
    if os.path.isfile(FILEPATH):
        register = read_register()
    else:
        register = dict()
    register[uf] = time
    f = open(FILEPATH, 'w')
    f.write(json.dumps(register))
