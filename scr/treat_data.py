import pandas as pd
import os

def detect_missing(df: pd.DataFrame()):
    if len(df) == 0:
        return pd.DataFrame(), pd.DataFrame()

    columns = list(df.columns)
    cd = df.groupby(columns).size()

    cd = pd.DataFrame(cd)
    cd.columns = ['contagem']
    cd = pd.DataFrame(cd).reset_index()

    mss = cd.copy()
    mss = mss[mss['contagem'] > 1]

    ddp = cd.copy()
    ddp = ddp[ddp['contagem'] == 1]
    ddp['contagem'] = 1
    gb = list(ddp.columns)
    gb.remove('contagem')
    ddp = pd.DataFrame(ddp.groupby(gb).sum())
    ddp = ddp.reset_index()

    return mss, ddp


def detect_wrong_date(df):
    if len(df) == 0:
        return pd.DataFrame(), pd.DataFrame()
    df['data_incorreta'] = False
    df.loc[df['data'] < '2021-01-17', 'data_incorreta'] = True
    wr = df[df['data_incorreta']].copy()
    df = df[~df['data_incorreta']].copy()
    del df['data_incorreta']
    return wr, df


def aggregate_count(df):
    if len(df) == 0:
        return pd.DataFrame(), pd.DataFrame()
    del df['paciente_id']
    del df['contagem']
    gb = df.groupby(list(df.columns)).size()
    df = pd.DataFrame(gb)
    df.columns = ['contagem']
    df = df.reset_index()
    columns = list(df.columns)
    columns.remove('contagem')
    df = df[columns + ['contagem']]
    df = df[['contagem'] + columns]
    df = df.sort_values(by=list(df.columns), ascending=False)
    return df


def separate_by_date(df):
    r = dict()
    gb = df.groupby('data')
    for g in gb:
        data = g[1]
        del data['data']
        r[g[0]] = data
    return r
