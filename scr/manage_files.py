import os

import pandas as pd

from SETUP import PATH_REPO, PATH_DATA

COMPRESSION = None


def get_directory_path(uf):
    return f'{PATH_REPO}/{PATH_DATA}/{uf}'


def _get_path(uf, date, subfolder=None):
    if subfolder is None:
        path = f'{get_directory_path(uf)}/{date}.csv'
    else:
        path = f'{get_directory_path(uf)}/{subfolder}/{date}.csv'
    if COMPRESSION is not None:
        path += f'.{COMPRESSION}'
    return path


def _read_file(uf, date):
    filepath = _get_path(uf, date)
    if os.path.isfile(filepath):
        return pd.read_csv(filepath, compression='zip')
    else:
        return pd.DataFrame()


def _merge_data(data_old, data_new):
    return data_new
    # This would only be needed if the data was get indexed by timestamp (which didn't work)
    # for df in [data_old, data_new]:
    #     df.set_index(['dose', 'age'], inplace=True)
    # return data_old.sum(data_new, fill_value=0).reindex(data_old.index)


def update_file(uf, date: str, data: pd.DataFrame, data_name: str=None):
    filepath = _get_path(uf, date, data_name)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not os.path.isfile(filepath):
        data.to_csv(filepath, compression=COMPRESSION, index=False)
    else:
        data_old = pd.read_csv(filepath, compression=COMPRESSION)
        data_updated = _merge_data(data_old, data)
        data_updated.to_csv(filepath, compression=COMPRESSION, index=False)
