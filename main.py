import pandas as pd
import pymysql as py

db_config = {
    'host':'localhost',
    'user':'root',
    'password':'123456',
    'database':'leitura'
}

def salvar_no_db(dados):
    conexao = py.connect(**db_config)
    cursor = conexao.cursor()
    
    try:
        sql = """
        INSERT INTO arquivos(CPF, Nome, Celular)
        VALUES(%s, %s, %s)
        """
        
        cel = dados['Celular1'] if not pd.isna(dados['Celular1']) else ''
        
        cursor.execute(sql, (dados['CPF'], dados['Nome'], cel))
        conexao.commit()
        
    except Exception as e:
        print(f"erro ao salvar no banco {e}")
    
    finally:
        cursor.close()
        conexao.cursor()

planilha_path = 'disparos08.11Higienizado.csv'
df = pd.read_csv(planilha_path, sep = ';')

print(df.columns)

for index, row in df.iterrows():
    # converte a linha pra json
    dados_json = row.to_dict()
    
    salvar_no_db(dados_json)
    
    # marcar como processado (adicionar uma coluna 'Processado', na planilha)
    # at para renomear uma coluna
    df.at[index, 'Processado'] = 'Sim'

df.to_csv(planilha_path , index = False)
print('Processo Concluído')








# print(df.shape)
# print(len(df))
# print(df)
# print(df.head())
# df.info()

# dtype para definir o tipo da coluna
# muitas colunas e quero ler só algumas = usecols=[nome_das_colunas, indices_das_colunas]
# list(df.cols) = a lista das colunas
# ler um número específico de linhas do arquivo = nrows / df = pd.read_csv('diegao.csv', sep=';', on_bad_lines='skip', nrows = 100)
# pular um número específico de linhas no início = skiprows = 5 (5primeiras)
#                                      no final = skipfooter = 5
# ler um bloco de arquivos = chunksize / df = pd.read_csv('diegao.csv', sep=';', on_bad_lines='skip', chunksize = 5000)
#  - pode iterar sobre o arquivo
#  - for chunk in df
#       print(chunk.head(2))   -   vai ler as duas primeiras linhas de todos os blocos de 5k

# linhas específicas
#     for index, row in df.iterrows():
#        print(f"Processando linha {index + 1}: {row['Nome']} - {row['Idade']} anos")