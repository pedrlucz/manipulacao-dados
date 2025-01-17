import pandas as pd
from datetime import datetime
import numpy as np
import mysql.connector
from mysql.connector import Error

class DataProcessor:
    def __init__(self):
        # Configurações do banco de dados
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'database': 'leitura_dados'
        }
        
        # Definição dos tipos esperados para cada tabela
        self.phones_dtypes = {
            'id': 'Int64',
            'stPhone': 'string',
            'lead_id': 'Int64',
            'blSanitized': 'boolean',
            'created_at': 'datetime64[ns]',
            'updated_at': 'datetime64[ns]'
        }
        
        self.leads_dtypes = {
            'id': 'Int64',
            'stName': 'string',
            'stCPF': 'string',
            'stRg': 'string',
            'dtBirth': 'datetime64[ns]',
            'stUF': 'string',
            'stCity': 'string',
            'stEmail': 'string',
            'stAddress': 'string',
            'stMothersName': 'string',
            'stFathersName': 'string',
            'blSanitized': 'Int64',
            'dtSanitized': 'datetime64[ns]',
            'created_at': 'datetime64[ns]',
            'updated_at': 'datetime64[ns]',
            'stCompany': 'string',
            'stOrgan': 'string',
            'dcSalary': 'float64',
            'blBlock': 'boolean',
            'dtReleasedIn': 'datetime64[ns]',
            'stNumber': 'string',
            'stDistrict': 'string',
            'stPosition': 'string',
            'stZipCode': 'string',
            'isATaker': 'boolean',
            'blBlackList': 'boolean'
        }

    def connect_to_database(self):
        """
        Estabelece conexão com o banco de dados
        """
        try:
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                return connection
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return None

    def save_to_database(self, df, table_name):
        """
        Salva o DataFrame no banco de dados
        """
        connection = self.connect_to_database()
        if connection is None:
            return False

        cursor = connection.cursor()
        try:
            # Prepara a query de inserção
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # Converte o DataFrame para uma lista de tuplas
            values = [tuple(x) for x in df.replace({pd.NaT: None, pd.NA: None}).values]

            # Executa a inserção em lotes
            for batch in self.chunks(values, 1000):  # Processa em lotes de 1000 registros
                cursor.executemany(query, batch)
                
            connection.commit()
            print(f"Dados salvos com sucesso na tabela {table_name}")
            return True

        except Error as e:
            print(f"Erro ao salvar no banco de dados: {e}")
            connection.rollback()
            return False

        finally:
            cursor.close()
            connection.close()

    def chunks(self, lst, n):
        """Divide uma lista em pedaços de tamanho n"""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def read_csv(self, file_path, table_type):
        """
        Lê o arquivo CSV e aplica as conversões necessárias
        """
        try:
            # Lê o CSV com ponto e vírgula como separador
            df = pd.read_csv(file_path, sep=';')
            
            # Seleciona os tipos corretos baseado na tabela
            dtypes = self.phones_dtypes if table_type == 'phones' else self.leads_dtypes
            
            # Converte os tipos de dados
            for column, dtype in dtypes.items():
                if column in df.columns:
                    try:
                        if dtype == 'datetime64[ns]':
                            df[column] = pd.to_datetime(df[column], errors='coerce')
                        else:
                            df[column] = df[column].astype(dtype)
                    except Exception as e:
                        print(f"Erro ao converter coluna {column}: {str(e)}")
                        if dtype == 'Int64':
                            df[column] = pd.NA
                        elif dtype == 'string':
                            df[column] = None
                        elif dtype == 'boolean':
                            df[column] = False
                        elif dtype == 'datetime64[ns]':
                            df[column] = pd.NaT
                        elif dtype == 'float64':
                            df[column] = np.nan

            return df
        
        except Exception as e:
            print(f"Erro ao ler arquivo: {str(e)}")
            return None

    def validate_data(self, df, table_type):
        """
        Valida os dados conforme regras de negócio
        """
        if table_type == 'leads':
            # Valida CPF obrigatório
            invalid_rows = df[df['stCPF'].isna()]
            if not invalid_rows.empty:
                print(f"Encontradas {len(invalid_rows)} linhas com CPF faltando")
                return False
            
        return True

    def process_file(self, file_path, table_type):
        """
        Processa o arquivo completo
        """
        # Lê e processa o arquivo
        df = self.read_csv(file_path, table_type)
        if df is None:
            return None
        
        # Valida os dados
        if not self.validate_data(df, table_type):
            return None
        
        # Adiciona timestamps se não existirem
        if 'created_at' not in df.columns:
            df['created_at'] = datetime.now()
        if 'updated_at' not in df.columns:
            df['updated_at'] = datetime.now()
            
        return df

# Exemplo de uso
def main():
    processor = DataProcessor()
    
    file_path = r'C:\leitura_a\leads_data'
    
    # Processa arquivo de leads
    leads_df = processor.process_file(file_path, 'leads')
    if leads_df is not None:
        print("Arquivo de leads processado com sucesso")
        if processor.save_to_database(leads_df, 'leads'):
            print("Leads salvos no banco com sucesso")
    try:
        with open(file_path, 'r') as f:
            print("Arquivo lido com sucesso pelo método `open`.")
    except Exception as e:
        print(f"Erro ao abrir arquivo com `open`: {e}")

        
    # # Processa arquivo de phones
    # phones_df = processor.process_file(r'C:\Users\ptest\Desktop\leitura_a\phone_data', 'phones')
    # if phones_df is not None:
    #     print("Arquivo de phones processado com sucesso")
    #     if processor.save_to_database(phones_df, 'phones'):
    #         print("Phones salvos no banco com sucesso")

if __name__ == "__main__":
    main()  