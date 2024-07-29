# Introdução

Este projeto foi desenvolvido para processar e analisar grandes arquivos CSV contendo dados de vendas. 
Utiliza o processamento em chunks para otimizar o uso da memória e calcula estatísticas agregadas sobre as vendas, 
como o produto mais vendido, o canal de vendas mais eficaz e o volume de vendas por país e região.

## Objetivo

O objetivo principal do projeto é fornecer uma solução eficiente para processar grandes volumes de dados de vendas, 
minimizando o uso de memória e proporcionando análises rápidas. As principais funcionalidades incluem:

- **Processamento em Chunks:** Divide arquivos grandes em pedaços menores para evitar o uso excessivo de memória.
- **Agregação de Dados:** Calcula totais de vendas por produto, canal de vendas, país e região.
- **Otimização de Tipos de Dados:** Converte colunas para tipos de dados apropriados para melhorar o uso da memória.
- **Relatórios de Uso de Recursos:** Monitora o uso de memória e o tempo de CPU durante o processamento.

## Desafio

O objetivo deste teste é avaliar habilidades em manipular e analisar grandes volumes de dados em Python, de forma eficiente e com consumo otimizado de recursos. 
Você receberá um arquivo CSV de grande porte, denominado `vendas.csv`, com cerca de 5GB. 
O arquivo contém dados de vendas de uma cadeia de varejo, com as seguintes colunas: Data, Produto, Quantidade, Preço_Unitário e Loja.

### Instruções

- Implemente uma solução para ler o arquivo `vendas.csv` de forma eficiente, considerando o grande volume de dados.
- Identifique o produto mais vendido em termos de quantidade e canal.
- Determine qual país e região teve o maior volume de vendas (em valor).
- Calcule a média de vendas mensais por produto, considerando o período dos dados disponíveis.

### Requisitos

- Sua solução deve ser capaz de rodar em uma máquina com memória limitada; não assuma que o arquivo inteiro pode ser carregado na memória de uma vez.
- Use técnicas como leitura em partes (chunking).
- Priorize a eficiência do processamento.
- O uso de bibliotecas como Pandas é permitido, especialmente com seu recurso de leitura em chunks.
- Documente seu código adequadamente e inclua comentários explicativos sobre suas escolhas de implementação.

## Requisitos de Execução

Para executar o processamento, é necessário ter o Python 3.10 instalado e garantir que o arquivo `vendas.csv` esteja salvo na pasta `data`.

### Instalando as Dependências e Executando o Código

1. Abra o terminal e instale as dependências necessárias com o seguinte comando:

    ```bash
    pip install -r requirements.txt
    ```

2. Ainda no terminal, inicie o processamento com o comando:

    ```bash
    python main.py
    ```
