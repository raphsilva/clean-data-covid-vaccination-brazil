import os
import pandas as pd


def _get_path(uf, date):
    return f'data/{uf}/{date}.csv'


def read_file(uf, date):
    filepath = _get_path(uf, date)
    if os.path.isfile(filepath):
        return pd.read_csv(filepath, compression='zip')
    else:
        return pd.DataFrame()


def merge_data(data_old, data_new):
    for df in [data_old, data_new]:
        df.set_index(['date_vaccine', 'dose', 'age'], inplace=True)
    return data_old.sum(data_new, fill_value=0).reindex(data_old.index)


def update_file(uf, date: str, data: pd.DataFrame):
    filepath = _get_path(uf, date)
    if not os.path.isfile(filepath):
        data.to_csv(compression=zip, index=False)
    else:
        data_old = pd.read_csv(filepath, compression='zip')
        data_updated = merge_data(data_old, data)
        data_updated.to_csv(compression=zip, index=False)