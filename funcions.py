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
                INSERT INTO leads (stCPF, stRg, dtBirth, stName, stEmail, stCity, stUF, stAddress, stMothersName, stFathersName, blSanitized, dtSanitized, stCompany, stOrgan, dcSalary, blBlock, dtReleasedIn, stNumber, stDistrict, stPosition, stZipCode, isATaker, blBlackList)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(sql_inserir_lead, (
                dados['stCPF'], dados.get('stRg'), dados.get('dtBirth'), dados['stName'], dados['stEmail'], 
                dados['stCity'], dados['stUF'], dados.get('stAddress'), dados.get('stMothersName'), dados.get('stFathersName'),
                dados.get('blSanitized'), dados.get('dtSanitized'), dados.get('stCompany'), dados.get('stOrgan'),
                dados.get('dcSalary'), dados.get('blBlock'), dados.get('dtReleasedIn'), dados.get('stNumber'),
                dados.get('stDistrict'), dados.get('stPosition'), dados.get('stZipCode'), dados.get('isATaker'), dados.get('blBlackList')
            ))
            
            conexao.commit()
            
            lead_id = cursor.lastrowid

        return lead_id

    except Exception as e:
        print(f"Erro ao salvar lead no banco: {e}")
        return None
        
    finally:
        cursor.close()
        conexao.close()

def salvar_telefone_no_db(lead_id, telefones):
    """Salva um número de telefone associado a um lead no banco de dados."""
    if telefones: # verifica se a lista de telefone nao está vazia
        # filtrando os telefones válidos, não nulos
        
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

def processar_planilha_indice(arquivo):
    """Irá processar o JSON pelo índice"""
    try:
        df = pd.read_csv(arquivo, sep = ';', encoding = 'utf-8')
    
    except Exception as e:
        print(f"Erro ao ler a planilha {arquivo}: {e}")
        return
    
    colunas = df.columns.tolist()
    print(f"colunas encontradas {colunas}")
    
    dados_json = df.to_dict(orient = 'records') # cada linha vai se tornar um dicionário
 
    for item in dados_json:
        # convertendo a lista de dicionários em json
        json_dados = json.dumps(item, ensure_ascii = False)
        print(f"JSON dos dados: {json_dados}")
        
        # acessando os valores por indíces
        valores = list(item.values()) # pega os valores na ordem das colunas
        
        # criando um dicionário para o banco de dados usando índices das colunas
        dados_lead = {
            'stCPF': item[colunas[0]],  
            'stRg': item[colunas[1]],  
            'dtBirth': item[colunas[2]],  
            'stName': item[colunas[3]],  
            'stEmail': item[colunas[4]],
            'stCity': item[colunas[5]],
            'stUF': item[colunas[6]],
            'stAddress': item[colunas[7]],
            'stMothersName': item[colunas[8]],
            'stFathersName': item[colunas[9]],
            'stSanitized': item[colunas[10]],
            'dtSanitized': item[colunas[11]],
            'stCompany': item[colunas[12]],
            'stOrgan': item[colunas[13]],
            'dcSalary': item[colunas[14]],
            'blBlock': item[colunas[15]],
            'dtReleasedIn': item[colunas[16]],
            'stNumber': item[colunas[17]],
            'stDistrict': item[colunas[18]],
            'stPosition': item[colunas[19]],
            'stZipCode': item[colunas[20]],
            'isATaker': item[colunas[21]],
            'blBlackList': item[colunas[22]]      
        }
        
        lead_id = salvar_lead_no_db(dados_lead)
        
        if lead_id:
            print(f"Lead {lead_id} salvo com sucesso.")
        else:
            print(f"Erro ao salvar o lead {item[colunas[0]]}")    
        
def processar_arquivos(pasta_leads):
    # pra processar os arquivos contidos nas pastas específicadas
    arquivos_leads = [f for f in os.listdir(pasta_leads) if f.endswith('.csv')] # o endswith pra filtrar os com extensão .csv
    
    # pra ler os dados de cada arquivo CSV e tranformar 
    for arquivo in arquivos_leads:
        print(f"lendo o arquivo de leads: {arquivo}")

        caminho_arquivo = os.path.join(pasta_leads, arquivo)
        
        try:
            df = pd.read_csv(caminho_arquivo, sep = ';', encoding = 'utf-8', engine = 'python')
            df = df.where(pd.notna(df), None)
            dados_json = df.to_dict(orient = "records")
            
        except Exception as e:
            print(f"erro ao ler a planilha {arquivo}: {e}")
        
        arquivo_processado_com_erro = False
        
        for item in dados_json:
            valores = list(item.values())
            
            if not valores[0]: # índice 0 deve conter CPF
                print(f"erro: CPF ausente no registro {valores}")
                arquivo_processado_com_erro = True
                continue
            
            telefones_validos = [
                valores[i] for i in range(10, 20)
                if valores[i] and isinstance(valores[i], (str, int))
            ]
            
            lead_id = salvar_lead_no_db(valores)
            
            if not lead_id:
                arquivo_processado_com_erro = True
                continue
            
            print(f"telefones para salvar no lead ID {lead_id}: {telefones_validos}")
            salvar_telefone_no_db(lead_id, telefones_validos)
            
        if arquivo_processado_com_erro:
            novo_nome = caminho_arquivo.replace('.csv', '_erro.csv')
            os.rename(caminho_arquivo, novo_nome)
            print(f"erro no arquivo {arquivo}, renomeado para {novo_nome}")
            
        else:
            os.remove(caminho_arquivo)
            print(f"arquivo {arquivo} processado e apagado")