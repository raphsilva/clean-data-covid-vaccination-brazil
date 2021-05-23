import os
from base64 import b64encode
from multiprocessing.pool import ThreadPool
from time import time

import pandas as pd
import requests
from elasticsearch import Elasticsearch, helpers

from SETUP import QUICK_TEST
from time_format import datetime_to_str

if QUICK_TEST:
    MAX_SIZE = 3
else:
    MAX_SIZE = 1000000

url = 'https://imunizacao-es.saude.gov.br/'
username = 'imunizacao_public'
password = 'qlto5t&7r_@+#Tlstigi'
client = Elasticsearch(url, http_auth=(username, password), maxsize=25, retry_on_timeout=True, max_retries=5)

userAndPass = b64encode((b"f'{username}:{password}'")).decode("ascii")
headers = {'Authorization': 'Basic %s' % userAndPass}

aggregators = ['vacina_dataAplicacao', 'paciente_idade', 'paciente_enumSexoBiologico', 'vacina_nome', 'paciente_id']


def get_data(uf, dose, date_A, date_B):
    pool = ThreadPool(processes=3)
    ages = [0, 25, 30, 40, 50, 60, 80, 1000]
    futures = list()
    reg = list()
    for i in range(len(ages) - 1):
        a = ages[i]
        b = ages[i + 1]
        args = (uf, dose, date_A, date_B, a, b)
        future = pool.apply_async(get_data_age, args)  # tuple of args for foo
        futures.append(future)
        reg.append(args)
    for i in range(len(futures)):
        try:
            futures[i].get()
        except:
            print('>>> ERROR')
            print()
            print('>>>', reg[i])
            print()
    data_parts = [v.get() for v in futures]
    return pd.concat(data_parts)


def __request_data(body, trial=1):
    print('Requesting data', body['query']['bool']['must'])
    if trial > 10:
        return None
    try:
        r = requests.post(url, json=body, auth=(username, password))
    except:
        print('Request did not suceed. Trial', trial)
        return __request_data(body, trial + 1)
    print('Got data', body['query']['bool']['must'])
    return r


def request_scan(body, trial=1):
    if trial > 5:
        return None
    try:
        resp = helpers.scan(
            client,
            scroll='3m',
            size=10000,
            query=body,
            request_timeout=5000
        )
    except:
        print('Request did not suceed. Trial', trial)
        return request_scan(body, trial + 1)
    return resp


pending = 0
pend_times = dict()
pend_args = dict()
id_d = 0


def get_data_age(uf, dose, date_A, date_B, age_A, age_B):
    global pending, pend_times, id_d, pend_args
    keys_to_keep = ['paciente_id', 'paciente_enumSexoBiologico', 'paciente_idade', 'vacina_nome', 'vacina_categoria_nome', 'vacina_grupoAtendimento_nome', 'vacina_dataAplicacao']
    body = {
        "query": {
            "bool": {
                "must": [{"term": {"paciente_endereco_uf": uf}},
                         {"regexp": {"vacina_descricao_dose": f'.*{dose}.*'}},
                         {"range": {"paciente_idade": {"gte": age_A, "lt": age_B}}},
                         {"range": {"vacina_dataAplicacao": {"gte": date_A, "lt": date_B}}},
                         ]
            },
        },
        "_source": keys_to_keep
    }
    pending += 1
    t0 = time()
    id_d += 1
    t_id = id_d
    pend_times[t_id] = t0
    pend_args[t_id] = (uf, dose, date_A, age_A)
    resp = request_scan(body)
    result = list(resp)
    data = [i['_source'] for i in result]
    df = pd.DataFrame(data)
    if len(df) == 0:
        df = pd.DataFrame(columns=keys_to_keep)
    df = df[keys_to_keep]
    df['data'] = df['vacina_dataAplicacao'].str[:10]
    del df['vacina_dataAplicacao']
    df['dose'] = dose
    pending -= 1

    r_times = list()
    del pend_times[t_id]
    queue_time = None
    blocker_args = None
    if len(pend_times) > 0:
        for i in pend_times:
            r_times.append(int(time() - pend_times[i]))
        queue_time = sorted(r_times, reverse=True)
        gt = max(r_times)
        for i in list(pend_times.keys()):
            if time() - pend_times[i] >= gt:
                blocker_args = pend_args[i]
                break

    print('Now being requested:', pending, '  Got data of length', len(df), '--', date_A, dose, age_A, f'{int(time() - t0)}s', queue_time, blocker_args)

    return df


def __get_data_age_agg(uf, dose, date_A, date_B, age_A, age_B):
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

    r = __request_data(body)

    data = r.json()
    if 'aggregations' not in data:
        print('\nERROR Could not get data')
        print(uf, dose, date_A, date_B, age_A, age_B)
        print(data)
        print()
        os._exit(1)

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
