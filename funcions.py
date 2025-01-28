from dbconfig import db_config 
import pymysql as py
import pandas as pd
import json
import os
""""""
def salvar_lead_no_db(dados):
    """Salva os dados de um lead no banco de dados."""
    conexao = py.connect(**db_config)
    cursor = conexao.cursor()

    try:
        # verificar se o CPF já existe, se exitir retorna o id
        sql_verificar = "SELECT id FROM leads WHERE stCPF = %s"
        cursor.execute(sql_verificar, (dados['stCPF'],))
        lead_existente = cursor.fetchone()

        if lead_existente:
            lead_id = lead_existente[0]
            
        else:
            # caso o cpf não exista, pra inserir novo lead
            
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

def salvar_telefone_no_db(lead_id, telefones):
    """Salva um número de telefone associado a um lead no banco de dados."""
    if telefones: # verifica se a lista de telefone nao está vazia
        #filtrando os telefones válidos, não nulos
        
        telefones_validos = [ telefone for telefone in telefones if pd.notna(telefone)]
        
        if telefones_validos:

            conexao = py.connect(**db_config)
            cursor = conexao.cursor()

            try:
                for telefone in telefones_validos:
                        
                    # onde insere o telefone na tabela phone
                    sql_inserir_telefone = """
                        INSERT INTO phones (stPhone, lead_id)
                        VALUES (%s, %s)
                    """
                    
                    cursor.execute(sql_inserir_telefone, (telefone, lead_id))
                
                    conexao.commit()

            except Exception as e:
                print(f"Erro ao salvar telefone no banco: {e}")
                
            finally:
                cursor.close()
                conexao.close()
        
        else:
            print(f"Nenhum telefone válido para salvar para o lead ID {lead_id}")

def converter_csv_json(caminho_csv):
    try:
        df = pd.read_csv(caminho_csv, sep = ";", encoding = "utf-8", engine = "python")
        df = df.where(pd.notna(df), None) #substitui o NaN por None
        return df.to_dict(orient = 'records') # converter para uma lista de dicionários
    
    except Exception as e:
        print(f"erro ao converter {caminho_csv} para json: {e}")

def processar_arquivos(pasta_leads):
    # pra processar os arquivos contidos nas pastas específicadas
    arquivos_leads = [f for f in os.listdir(pasta_leads) if f.endswith('.csv')] # o endswith pra filtrar os com extensão .csv
    
    # pra ler os dados de cada arquivo CSV e tranformar 
    for arquivo in arquivos_leads:
        print(f"lendo o arquivo de leads: {arquivo}")

        caminho_arquivo = os.path.join(pasta_leads, arquivo)
        dados_leads = converter_csv_json(caminho_arquivo)
        
        if not dados_leads:
            continue
        
        arquivo_processado_com_erro = False
        
        for lead in dados_leads:
            if not lead.get('stCPF'):
                print(f"erro: CPF ausente no registro {lead}")
                
                arquivo_processado_com_erro = True
                
                continue
            
            telefones = [ telefone for telefone in
                [ 
                         lead.get(f"telefone{i}") for i in range(1, 11)
                            ]
            if pd.notna(telefone)    ]
            
            telefones_validos = [ t for t in telefones if t ] # remove valores vazios            
            
            lead_id = salvar_lead_no_db(lead)
            
            if not lead_id:
                arquivo_processado_com_erro = True
                continue
            
            print(f'Telefones para salvar no lead ID {lead_id}: {telefones_validos}')
            salvar_telefone_no_db(lead_id, telefones_validos)
        
        # renomear
        if arquivo_processado_com_erro:
            novo_nome = caminho_arquivo.replace('.csv', 'erro.csv')
            os.rename(caminho_arquivo, novo_nome)
            print(f"erro no arquivo {arquivo}, renomeado para {novo_nome}")
        
        else:
            os.remove(caminho_arquivo)
            print(f'arquivo {arquivo} processado e apagado.')