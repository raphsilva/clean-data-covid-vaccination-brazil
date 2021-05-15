from base64 import b64encode
from multiprocessing.pool import ThreadPool

import pandas as pd
import requests

from SETUP import QUICK_TEST
from time_format import datetime_to_str

if QUICK_TEST:
    MAX_SIZE = 3
else:
    MAX_SIZE = 10000

url = 'https://imunizacao-es.saude.gov.br/_search'
username = 'imunizacao_public'
password = 'qlto5t&7r_@+#Tlstigi'

userAndPass = b64encode((b"f'{username}:{password}'")).decode("ascii")
headers = {'Authorization': 'Basic %s' % userAndPass}

aggregators = ['vacina_dataAplicacao', 'paciente_idade', 'paciente_enumSexoBiologico', 'vacina_nome', 'paciente_id']


def get_data(uf, dose, date_A, date_B):
    pool = ThreadPool(processes=10)
    ages = [0, 10, 20, 30, 40, 50, 60, 70, 90, 110, 1000]
    futures = list()
    for i in range(len(ages) - 1):
        a = ages[i]
        b = ages[i + 1]
        future = pool.apply_async(get_data_age, (uf, dose, date_A, date_B, a, b))  # tuple of args for foo
        futures.append(future)

    data_parts = [v.get() for v in futures]
    return pd.concat(data_parts)

    return r


def get_data_age(uf, dose, date_A, date_B, age_A, age_B):
    def unroll(aggregators, data, partial={}, unrolled=[]):
        agg = aggregators[0]
        aggregators = aggregators[1:]
        for a in data[agg]['buckets']:
            partial[agg] = a['key']
            if len(aggregators) == 0:
                partial['contagem'] = a['doc_count']
                unrolled.append(dict(partial))
            else:
                unrolled = unroll(aggregators, a, partial, unrolled)
        return unrolled

    def make_aggdic(aggtors):
        if len(aggtors) == 1:
            return {
                aggtors[0]: {
                    "terms": {
                        "field": aggtors[0],
                        "size": MAX_SIZE
                    }
                }
            }
        return {
            aggtors[0]: {
                "terms": {
                    "field": aggtors[0],
                    "size": MAX_SIZE
                },
                "aggs": make_aggdic(aggtors[1:])
            }
        }

    body = {
        "aggs": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [{"term": {"paciente_endereco_uf": uf}},
                                 {"regexp": {"vacina_descricao_dose": f'.*{dose}.*'}},
                                 {"range": {"paciente_idade": {"gte": age_A, "lt": age_B}}},
                                 {"range": {"vacina_dataAplicacao": {"gte": date_A, "lt": date_B}}},
                                 ]
                    },
                },
                "aggs": make_aggdic(aggregators)
            }
        },
        'size': 0
    }

    r = requests.post(url, json=body, auth=(username, password))

    data = r.json()
    if 'aggregations' not in data:
        print('Could not get data', data)
        return pd.DataFrame()

    u = unroll(aggregators, data['aggregations']['filtered'])
    df = pd.DataFrame(u)

    if len(df) == 0:
        return pd.DataFrame()

    df = df.sort_values(by='contagem', ascending=False)
    df['data'] = pd.to_datetime(df['vacina_dataAplicacao'], unit='ms').apply(lambda x: datetime_to_str(x))
    del df['vacina_dataAplicacao']
    df['dose'] = dose

    return df


def get_data_uf(uf, date_A, date_B):
    pool = ThreadPool(processes=2)
    t1 = pool.apply_async(get_data, (uf, '1', date_A, date_B))  # tuple of args for foo
    t2 = pool.apply_async(get_data, (uf, '2', date_A, date_B))  # tuple of args for foo
    d1 = t1.get()
    d2 = t2.get()
    d1['dose'] = '1'
    d2['dose'] = '2'
    return pd.concat([d1, d2])
