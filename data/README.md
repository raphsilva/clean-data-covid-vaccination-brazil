# Dados: imunização de covid-19 no Brasil

Este diretório centraliza os dados coletados e processados. 
Este diretório está organizado em subdiretórios que contêm dados relativos às unidades federativas. 
Dentro de cada subdiretório, há as seguintes seções:

* `_info`: Informações gerais sobre os dados coletados. 
* `consistent`: Dados agregados contemplando os microdados para os quais não foram detectadas inconsistências de informações. 
* `inconsistent`: Dados agregados contemplando os microdados para os quais foram detectadas inconsistências de informações. 
* `wrong_date`: Dados agregados contemplando os microdados para os quais não foram detectadas inconsistências de informações exceto pela data, que está fora do real período de vacinação.

As seções abaixo descrevem como ler esses dados:

## `_info`

O diretório `_info` contém:

* Um arquivo `totals.csv` que contém o número total de vacinas aplicadas em cada data, sumarizando os dados disponíveis neste repositório. 
* Um diretório `update_totals`, que contém um arquivo para cada data de aplicação de vacina. Cada arquivo registra as datas em que os dados correspondente foram atualizados, e a contagem total de vacinas aplicadas em cada data de atualização.

## `consistent`

O diretório `consistent` contém os dados agregados. Ele contém vários arquivos CSV cujos nomes são correspondentes às datas de aplicação. Cada arquivo contém os seguintes campos, nomeados conforme a documentação do OpenDataSus: 
* paciente_enumSexoBiologico
* paciente_idade 
* vacina_nome
* vacina_categoria_nome
* vacina_grupoAtendimento_nome

Além desses campos, há uma coluna `dose` que informa com um número inteiro se a dose aplicada foi a primeira ou a segunda. 

A coluna `contagem` informa quantas vezes a combinação de dados daquela linha ocorreu na data de vacinação correspondente. 

## `inconsistent`

O diretório `inconsistent` contém dados com possíveis problemas de consistência. Eles estão dispostos da mesma forma que os do diretório `consistent`, exceto que contêm uma coluna extra chamada `paciente_id` para facilitar a análise de IDs duplicados. 

## `wrong_date`

O diretório `wrong_date` contém os dados cujas datas estão claramente erradas. Sua organização é idêntica ao do diretório `consistent`.