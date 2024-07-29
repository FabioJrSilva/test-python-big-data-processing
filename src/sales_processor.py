import pandas as pd
import psutil
import time

class SalesProcessor:
    def __init__(self, file_path, chunksize=10**6, sample_fraction=0.1):
        """
        Inicializa o processador de vendas.

        Parameters:
        - file_path: Caminho para o arquivo CSV de vendas.
        - chunksize: Número de linhas por chunk a serem lidas de cada vez.
        - sample_fraction: Fração de amostra do arquivo a ser carregada.
        """
        self.file_path = file_path
        self.chunksize = chunksize
        self.sample_fraction = sample_fraction

        # Dicionários para armazenar os totais de vendas
        self.product_sales = {}
        self.sales_channel_sales = {}
        self.country_sales = {}
        self.region_sales = {}
        self.monthly_sales = {}

    def _update_sales_totals(self, chunk, group_by_column, sales_dict, value_column):
        """
        Atualiza os totais de vendas para a coluna especificada no DataFrame.

        Parameters:
        - chunk: DataFrame contendo dados de vendas.
        - group_by_column: Nome da coluna para agrupamento e agregação.
        - sales_dict: Dicionário onde as chaves são os valores únicos da coluna
                      e os valores são as quantidades totais vendidas.
        - value_column: Nome da coluna cujos valores serão somados.
        """
        # Agrupa e calcula o valor total
        group = chunk.groupby(group_by_column, observed=True).agg({value_column: 'sum'})

        # Atualiza o dicionário de vendas
        for key, value in group[value_column].items():
            if key not in sales_dict:
                sales_dict[key] = 0
            sales_dict[key] += value

    def _print_resource_usage(self, step):
        """
        Imprime o uso de recursos de memória e CPU.

        Parameters:
        - step: Identificador do chunk sendo processado.
        """
        process = psutil.Process()
        mem_info = process.memory_info()
        print(f"Chunk: {step}")
        print(f"Memória usada: {mem_info.rss / (1024 * 1024):.2f} MB")
        print(f"Tempo de CPU: {process.cpu_times().user:.2f} segundos\n")

    def _process_chunk(self, chunk, chunk_number):
        """
        Processa um chunk de dados de vendas.

        Parameters:
        - chunk: DataFrame do chunk atual.
        - chunk_number: Número do chunk atual.
        """
        # Padroniza os nomes das colunas
        chunk.columns = chunk.columns.str.strip().str.replace(' ', '_').str.lower()

        # Ajustando os tipos inteiros
        ints = chunk.select_dtypes(include=['int64', 'int32', 'int16']).columns
        chunk[ints] = chunk[ints].apply(pd.to_numeric, downcast='integer')

        # Ajustando os tipos floats
        floats = chunk.select_dtypes(include=['float']).columns
        chunk[floats] = chunk[floats].apply(pd.to_numeric, downcast='float')

        # Padronizando as datas
        chunk['order_date'] = pd.to_datetime(chunk['order_date'], errors='coerce')
        chunk['ship_date'] = pd.to_datetime(chunk['ship_date'], errors='coerce')

        # O tipo Object é o que requer mais memória realiando uma transformação deste tipo para Category teremos uma redução muito grande dos recursos consumidos.
        objects = chunk.select_dtypes('object').columns
        chunk[objects] = chunk[objects].apply(lambda x: x.astype('category'))

        # Agrupa por produto e calcula a quantidade total vendida
        self._update_sales_totals(chunk=chunk, group_by_column='item_type', sales_dict=self.product_sales, value_column='units_sold')

        # Agrupa por canal de vendas e calcula a quantidade total vendida
        self._update_sales_totals(chunk=chunk, group_by_column='sales_channel', sales_dict=self.sales_channel_sales, value_column='units_sold')

        # Agrupa por país e calcula o volume de vendas (em valor total)
        self._update_sales_totals(chunk=chunk, group_by_column='country', sales_dict=self.country_sales, value_column='total_revenue')

        # Agrupa por região e calcula o volume de vendas (em valor total)
        self._update_sales_totals(chunk=chunk, group_by_column='region', sales_dict=self.region_sales, value_column='total_revenue')

        # Agrupa por mês e produto e calcula a média de vendas mensais
        chunk['month'] = chunk['order_date'].dt.to_period('M')
        monthly_group = chunk.groupby(['item_type', 'month'], observed=True)['total_revenue'].sum()

        for (product, month), total in monthly_group.items():
            if (product, month) not in self.monthly_sales:
                self.monthly_sales[(product, month)] = [0, 0]  # [total, count]

            self.monthly_sales[(product, month)][0] += total
            self.monthly_sales[(product, month)][1] += 1

        # Printa memória RAM utilizada e tempo de CPU para processar o chunk atual
        self._print_resource_usage(chunk_number)

    def process_sales_data(self):
        
        # Processa os dados de vendas, lendo e agregando os dados em chunks.

        # Medindo o tempo total de execução
        start_time = time.time()

        chunk_number = 0

        for chunk in pd.read_csv(self.file_path, chunksize=self.chunksize):
            chunk_number += 1
            self._process_chunk(chunk, chunk_number)

        total_time = time.time() - start_time
        print(f"Tempo total de execução: {total_time:.2f} segundos")

    def print_summary(self):
        # Imprime um resumo das vendas processadas.        
        most_sold_product = max(self.product_sales, key=self.product_sales.get)
        print(f"Produto mais vendido: {most_sold_product}, Quantidade: {self.product_sales[most_sold_product]:,}\n\n")

        most_sold_channel = max(self.sales_channel_sales, key=self.sales_channel_sales.get)
        print(f"Canal de vendas mais vendido: {most_sold_channel}, Quantidade: {self.sales_channel_sales[most_sold_channel]:,}\n\n")

        top_country = max(self.country_sales, key=self.country_sales.get)
        print(f"País com maior volume de vendas: {top_country}, Valor Total: {self.country_sales[top_country]:,.2f}\n\n")

        top_region = max(self.region_sales, key=self.region_sales.get)
        print(f"Região com maior volume de vendas: {top_region}, Valor Total: {self.region_sales[top_region]:,.2f}\n\n")

        # Calcula a média de vendas mensais por produto
        monthly_avg_sales = {k: v[0] / v[1] for k, v in self.monthly_sales.items()}
        print(f"Média de vendas mensais por produto: {monthly_avg_sales}")

