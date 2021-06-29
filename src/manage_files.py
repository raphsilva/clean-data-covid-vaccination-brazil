import os
import shutil
from time import sleep, time
import threading


import pandas as pd

from SETUP import PATH_REPO, PATH_DATA
from time_format import get_today_str
from time_format import get_week

COMPRESSION = None


def get_directory_path(uf, subfolder=None):
    if subfolder is None:
        r = f'{PATH_REPO}/{PATH_DATA}/{uf}'
    else:
        r = f'{PATH_REPO}/{PATH_DATA}/{uf}/{subfolder}'
    os.makedirs(r, exist_ok=True)
    return r


def _get_path(uf, date, subfolder=None):
    date_w = get_week(date)
    directory = get_directory_path(uf, subfolder)
    path = f'{directory}/{date_w}.csv'
    if COMPRESSION is not None:
        path += f'.{COMPRESSION}'
    return path


# def _read_file(uf, date):
#     filepath = _get_path(uf, date)
#     if os.path.isfile(filepath):
#         return pd.read_csv(filepath, compression='zip')
#     else:
#         return pd.DataFrame()

lock = threading.Lock()
def _update_file(filepath, data, remove_duplicates=[]):
    t0 = time()
    with lock:
        print('got lock  ', round(time()-t0, 2), filepath)
        if not os.path.isfile(filepath):
            data.to_csv(filepath, compression=COMPRESSION, index=False)
        else:
            data_old = pd.read_csv(filepath, compression=COMPRESSION)
            data_updated = pd.concat([data_old, data])
            if len(remove_duplicates) > 0:
                data_updated = data_updated.drop_duplicates(remove_duplicates, keep='last')
                data_updated = data_updated.sort_values(by=remove_duplicates, ascending=False)
            data_updated.to_csv(filepath, compression=COMPRESSION, index=False)


def _delete_file(filepath):
    shutil.move(filepath, f'/tmp/{filepath}')


def _save_file(filepath, data):
    data.to_csv(filepath, compression=COMPRESSION, index=False)


def update_file_uf_date(uf, date: str, data: pd.DataFrame, data_name: str = None):
    filepath = _get_path(uf, date, data_name)
    _update_file(filepath, data, remove_duplicates=['paciente_enumSexoBiologico', 'paciente_idade', 'vacina_nome', 'vacina_categoria_nome', 'vacina_grupoAtendimento_nome', 'dose', 'data_aplicaçao'])

def update_info_updates(uf, date: str, data: pd.DataFrame, data_name: str, spent_time):
    date_w = get_week(date)
    filepath = _get_path(uf, date_w, '_info/updates_totals')
    total = data['contagem'].sum()
    info = {'data_atualizacao': get_today_str(),
            'data_aplicaçao': date,
            'tipo': data_name,
            'media_tempo_gasto': spent_time,
            'total': total
            }
    print('Updating file:', filepath)
    _update_file(filepath, pd.DataFrame([info]), ['data_atualizacao', 'data_aplicaçao', 'tipo'])

    # SUMMARY:
    filepath = get_directory_path(uf, '_info') + '/totals.csv'
    info = {'data_aplicaçao': date,
            'data_atualizacao': get_today_str(),
            'tipo': data_name,
            'media_tempo_gasto': spent_time,
            'total': total
            }
    _update_file(filepath, pd.DataFrame([info]), ['data_aplicaçao', 'tipo'])

def read_info_updates(uf):
    filepath = get_directory_path(uf, '_info') + '/totals.csv'
    data = pd.read_csv(filepath, compression=COMPRESSION)
    return data
