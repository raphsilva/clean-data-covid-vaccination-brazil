import os

import pandas as pd

COMPRESSION = None


def _get_path(uf, date):
    if COMPRESSION is None:
        return f'data/{uf}/{date}.csv'
    if COMPRESSION == 'zip':
        return f'data/{uf}/{date}.csv.zip'


def _read_file(uf, date):
    filepath = _get_path(uf, date)
    if os.path.isfile(filepath):
        return pd.read_csv(filepath, compression='zip')
    else:
        return pd.DataFrame()


def _merge_data(data_old, data_new):
    for df in [data_old, data_new]:
        df.set_index(['dose', 'age'], inplace=True)
    return data_old.sum(data_new, fill_value=0).reindex(data_old.index)


def update_file(uf, date: str, data: pd.DataFrame):
    filepath = _get_path(uf, date)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not os.path.isfile(filepath):
        data.to_csv(filepath, compression=COMPRESSION, index=False)
    else:
        data_old = pd.read_csv(filepath, compression=COMPRESSION)
        data_updated = _merge_data(data_old, data)
        data_updated.to_csv(filepath, compression=COMPRESSION, index=False)
