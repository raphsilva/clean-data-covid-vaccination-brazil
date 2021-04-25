import requests
import json
import pandas as pd
from base64 import b64encode
from pprint import pprint

url = 'https://imunizacao-es.saude.gov.br/_search'
username = 'imunizacao_public'
password = 'qlto5t&7r_@+#Tlstigi'

userAndPass = b64encode((b"f'{username}:{password}'")).decode("ascii")
headers = { 'Authorization' : 'Basic %s' %  userAndPass }

aggregators = ['vacina_dataAplicacao', 'paciente_idade']


def get_data(uf, dose, timestamp):


    def make_aggdic(aggtors):
        if len(aggtors) == 1:
            return {
            aggtors[0]: {
                "terms": {
                    "field": aggtors[0],
                    "size": 3
                }
            }
        }
        return {
            aggtors[0]: {
                "terms": {
                    "field": aggtors[0],
                    "size": 3
                },
                "aggs": make_aggdic(aggtors[1:])
            }
        }

    body = {
      "aggs": {
        "filtered": {
          "filter": {
            "bool": {
              "must": [{ "term": {"paciente_endereco_uf": uf } },
                       {"regexp": {"vacina_descricao_dose": f'.*{dose}.*' }},
                       {"range": {"@timestamp": {"gte": timestamp}    }}]
            },
          },
          "aggs": make_aggdic(aggregators)
        }
      },
      "size": 1
    }

    r = requests.post(url, json=body, auth=(username, password))

    data = r.json()
    aggregated = list()
    for b in data['aggregations']['filtered']['vacina_dataAplicacao']['buckets']:
        date = b['key']
        for c in b['paciente_idade']['buckets']:
            age = c['key']
            count = c['doc_count']
            aggregated.append({'uf': uf, 'date_vaccinated': date, 'age': age, 'count': count})
    df = pd.DataFrame(aggregated)
    df['date_vaccinated'] = pd.to_datetime(df['date_vaccinated'], unit='ms')
    df['date_vaccinated'] = df['date_vaccinated'].astype(str).str[:10]
    df = df[['date_vaccinated', 'age', 'count']]

    # combinar datas repetidas
    g = df.groupby(['date_vaccinated', 'age']).sum()

    return g