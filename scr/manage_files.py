import os

import pandas as pd
from time_format import get_today_str

from SETUP import PATH_REPO, PATH_DATA

COMPRESSION = None


def get_directory_path(uf, subfolder=None):
    if subfolder is None:
        r = f'{PATH_REPO}/{PATH_DATA}/{uf}'
    else:
        r = f'{PATH_REPO}/{PATH_DATA}/{uf}/{subfolder}'
    os.makedirs(r, exist_ok=True)
    return r


def _get_path(uf, date, subfolder=None):
    directory = get_directory_path(uf, subfolder)
    path = f'{directory}/{date}.csv'
    if COMPRESSION is not None:
        path += f'.{COMPRESSION}'
    return path


def _read_file(uf, date):
    filepath = _get_path(uf, date)
    if os.path.isfile(filepath):
        return pd.read_csv(filepath, compression='zip')
    else:
        return pd.DataFrame()


def _update_file(filepath, data, remove_duplicates=[]):
    if not os.path.isfile(filepath):
        data.to_csv(filepath, compression=COMPRESSION, index=False)
    else:
        data_old = pd.read_csv(filepath, compression=COMPRESSION)
        data_updated = pd.concat([data_old, data])
        if len(remove_duplicates) > 0:
            data_updated = data_updated.drop_duplicates(remove_duplicates, keep='last')
            data_updated = data_updated.sort_values(by=remove_duplicates, ascending=False)
        data_updated.to_csv(filepath, compression=COMPRESSION, index=False)


def _save_file(filepath, data):
    data.to_csv(filepath, compression=COMPRESSION, index=False)


def update_file_uf_date(uf, date: str, data: pd.DataFrame, data_name: str = None):
    filepath = _get_path(uf, date, data_name)
    _update_file(filepath, data)


def update_info_updates(uf, date: str, data: pd.DataFrame, data_name: str, spent_time):
    filepath = _get_path(uf, date, '_info/updates_totals')
    total = data['contagem'].sum()
    info = {'data_atualizacao': get_today_str(),
            'tipo': data_name,
            'media_tempo_gasto': spent_time,
            'total': total
            }
    _update_file(filepath, pd.DataFrame([info]), ['data_atualizacao', 'tipo'])

    #SUMMARY:
    filepath = get_directory_path(uf, '_info') + '/totals.csv'
    info = {'data_aplicaçao': date,
            'tipo': data_name,
            'media_tempo_gasto': spent_time,
            'total': total
            }
    _update_file(filepath, pd.DataFrame([info]), ['data_aplicaçao', 'tipo'])