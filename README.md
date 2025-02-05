## Pipeline de Transformação e Análise de Dados - Aprimorado

Este repositório contém um pipeline de dados aprimorado, construído usando SQL e dbt, para transformar e analisar dados de consumo de vídeo, incluindo o consumo de trailers. O pipeline ingere dados de várias fontes, realiza limpeza e transformações, e cria tabelas agregadas para análise. Ele também incorpora funções definidas pelo usuário (UDFs) para higienização de dados.

## Arquitetura

O pipeline mantém uma estrutura modular, com o gerenciamento de dependências e ordem de execução.

## Processos
1- Criação das tabelas.
2- Tratamento nos dados.
3- Inserção dos dados.
4- Checagem de completude nos dados.


## Higienização de Texto

* A UDF `UDF.GET_SANITIZED_TEXT` é usada para limpar e padronizar campos de texto, garantindo a consistência dos dados.
* Essa função é aplicada a campos como `video_name` para remover caracteres especiais, espaços extras e outras inconsistências.

## Geração de ID Aprimorada

* A geração de IDs foi aprimorada para usar hash SHA256 e incluir campos mais relevantes, garantindo a unicidade e a integridade dos IDs.

## Contato
darlene.rno@gmail.com
