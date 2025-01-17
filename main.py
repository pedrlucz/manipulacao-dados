import pandas as pd
import pymysql as py
import os

# Configurações do banco de dados
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'leitura'
}

def salvar_lead_no_db(dados):
    """Salva os dados de um lead no banco de dados."""
    conexao = py.connect(**db_config)
    cursor = conexao.cursor()

    try:
        # verificar se o CPF já existe
        sql_verificar = "SELECT id FROM leads WHERE stCPF = %s"
        cursor.execute(sql_verificar, (dados['stCPF'],))
        lead_existente = cursor.fetchone()

        if lead_existente:
            lead_id = lead_existente[0]
        else:
            # Inserir novo lead
            sql_inserir_lead = """
                INSERT INTO leads (stCPF, stName, stEmail, stCity, stUF)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql_inserir_lead, (
                dados['stCPF'],
                dados['stName'],
                dados['stEmail'],
                dados['stCity'],
                dados['stUF']
            ))
            
            conexao.commit()
            lead_id = cursor.lastrowid

        return lead_id

    except Exception as e:
        print(f"Erro ao salvar lead no banco: {e}")
        
    finally:
        cursor.close()
        conexao.close()

def salvar_telefone_no_db(lead_id, telefone):
    """Salva um número de telefone associado a um lead no banco de dados."""
    if not telefone:
        return

    conexao = py.connect(**db_config)
    cursor = conexao.cursor()

    try:
        # inserir telefone
        sql_inserir_telefone = """
            INSERT INTO phones (stPhone, lead_id)
            VALUES (%s, %d)
        """
        cursor.execute(sql_inserir_telefone, (telefone, lead_id))
        conexao.commit()

    except Exception as e:
        print(f"Erro ao salvar telefone no banco: {e}")
    finally:
        cursor.close()
        conexao.close()

def processar_arquivos(pasta_leads, pasta_phones):
    arquivos_leads = [f for f in os.listdir(pasta_leads) if f.endswith('.csv')]
    arquivos_phones = [f for f in os.listdir(pasta_phones) if f.endswith('.csv')]
    
    # processar os arquivos de leads
    for arquivo in arquivos_leads:
        print(f"lendo o arquivo de leads: {arquivo}")

        caminho_arquivo_leads = os.path.join(pasta_leads, arquivo)
        
        try:
            leads_df = pd.read_csv(caminho_arquivo_leads, sep = ';', encoding = 'utf-8', engine = 'python')
        
        except Exception as e:
            print(f'erro ao ler o arquivo de leads {arquivo}: {e}')
            continue
        
        arquivo_processado_com_erro = False
        
        # processar cada lead
        for _, lead in leads_df.iterrows():
            lead_dados = lead.where(pd.notna(lead), None).to_dict()
            if not lead_dados.get('stCPF'):
                print(f'erro: CPF não encontrado pro registro {lead_dados}')
                arquivo_processado_com_erro = True
                
            lead_id = salvar_lead_no_db(lead_dados)
            if not lead_id: # se o lead não for salvo corretamente
                arquivo_processado_com_erro = True
            
        # renomear o arquivo caso tenha dado erro
        if arquivo_processado_com_erro:
            novo_nome_arquivo = caminho_arquivo_leads.replace('.csv', "_erro.csv")
            os.rename(caminho_arquivo_leads, novo_nome_arquivo)
            print(f'erro no arquivo de leads, o arquivo foi renomeado para {novo_nome_arquivo}')
        
        else:
            # apagar o arquivo
            os.remove(caminho_arquivo_leads)
            print(f'arquivo {arquivo} de leads apagado')
    
    for arquivo in arquivos_phones:
        print(f'lendo o arquivo de phones: {arquivo}')
        
        caminho_arquivo_phones = os.path.join(pasta_phones, arquivo)
        
        try:
            phones_df = pd.read_csv(caminho_arquivo_phones, sep = ';', encoding = 'utf-8', engine = 'python')
        
        except Exception as e:
            print(f'erro ao ler o arquivo de phones {arquivo}: {e}')
            continue
        
        arquivo_processado_com_erro = False
        
        # pra processar cada telefone
        for _, telefone in phones_df.iterrows():
            lead_id = telefone.get('lead.id')
            if lead_id:
                sucesso = salvar_telefone_no_db(lead_id, telefone.get('stPhone'))
                if not sucesso:
                    arquivo_processado_com_erro = True
        
        # renomear o arquivo caso tenha acontecido um erro
        if arquivo_processado_com_erro:
            novo_nome_arquivo = caminho_arquivo_phones.replace(".csv", "_erro.csv")
            os.rename(caminho_arquivo_phones, novo_nome_arquivo)
            print(f"erro no arquivo de phones, o arquivo foi renomeado pra {novo_nome_arquivo}") 
        
        else:
            # apagar o arquivo
            os.remove(caminho_arquivo_phones)
            print(f"arquivo {arquivo} de phones apagado")

print('Processo Iniciado')          
            
        
pasta_leads = r'.\leituraleads'
pasta_phones = r'.\leituraphone'

processar_arquivos(pasta_leads, pasta_phones)

# listar todos os arquivos CSV na pasta 
# arquivos_leads = [f for f in os.listdir(pasta_leads) if f.endswith('.csv')]
# arquivos_phones = [f for f in os.listdir(pasta_phones) if f.endswith('.csv')]

# for arquivo in arquivos_leads:
#     print(f"lendo o arquivo de Leads: {arquivo}")
    
#     # caminho completo do arquivo CSV
#     caminho_arquivo_leads = os.path.join(pasta_leads, arquivo)
    
#     try:
#         # carregar os dados dos do arquivo CSV
#         leads_df = pd.read_csv(caminho_arquivo_leads, sep = ';', encoding = 'utf-8', engine = 'python')
#     except Exception as e:
#         print(f'erro ao ler o arquivo de Leads {arquivo}: {e}')
#         continue
    
#     # processar cada lead
#     for _, lead in leads_df.iterrows():
#         lead_dados = lead.where(pd.notna(lead), None).to_dict() # substituir o NaN por None
#         if not lead_dados.get('stCPF'): # verificar se tem CPF
#             print(f'erro: CPF não encontrado pro registro: {lead_dados}')
#             continue
    
#         lead_id = salvar_lead_no_db(lead_dados)
    
#     os.remove(caminho_arquivo_leads)
#     print(f'arquivo {arquivo} de leads apagado.')

# # path das planilhas
# leads_path = r'.\leituraleads\Leads.csv'
# phones_path = r'.\leituraphone\Phones.csv'

# # pra carregar os dados das planilhas
# leads_df = pd.read_csv(leads_path, sep = ';', encoding = 'utf-8', engine = 'python')
# phones_df = pd.read_csv(phones_path, sep = ';', encoding = 'utf-8', engine = 'python')

# processar arquivos de telefones
# for arquivo in arquivos_phones:
#     print(f'lendo o arquivo de phones {arquivo}')

#     caminho_arquivo_phones = os.path.join(pasta_phones, arquivo)
    
#     #carregar os dados do arquivo csv de telefones
#     try:
#         phones_df = pd.read_csv(caminho_arquivo_phones, sep = ';', encoding = 'utf-8', engine = 'python')
    
#     except Exception as e:
#         print(f'erro ao ler o arquivo de phones{arquivo}: {e}')
#         continue
    
#     for _, telefone in phones_df.iterrows():
#         lead_id = telefone.get('lead_id')
#         if lead_id: # ve se o tem o lead id
#             salvar_telefone_no_db(lead_id, telefone.get('stPhone'))
            
#     os.remove(caminho_arquivo_phones)
#     print(f"arquivo {arquivo} de phones apagado")

print('Processo Concluído')