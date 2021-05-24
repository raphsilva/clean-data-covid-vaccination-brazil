# Código-fonte: coleta e organização de dados de imunização de covid-19 no Brasil

Este código-fonte tem integração com a API do OpenDataSus e automatiza o acesso de dados, organização dos microdados em categorias e separação de dados inconsistentes devido a duplicação de IDs. 

## Funcionalidades

Algumas funcionalidades implementadas são:

* Integração com a API da OpenDataSus por meio de consulta do ElasticSearch em nível de microdados. Em princípio, tentou-se usar consultas agregadas para otimizar a busca, mas não foi possível continuar com essa estratégia por causa do problema com IDs duplicados (os resultados da busca agregada contariam as duplicatas pois os IDs seriam desconsiderados).

* Subdivisão da consulta em várias menores, para respeitar as limitações de tamanho da API.

* Multiprocessamento, para fazer várias requisições ao mesmo tempo. 

* Gerenciamento de arquivo, para saber quais datas precisam ser atualizadas. 

* Rotina para separar dados potencialmente inconsistentes (devido a duplicidade de IDs) e datas seguramente erradas (registros com data de antes do início da vacinação).
  
* Integração com o GitHub, para publicar automaticamente os dados processados (as chaves de acesso são privadas e não estão neste repositório).