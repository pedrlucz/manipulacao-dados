# from funcions import processar_arquivos

from dbconfig import db_config 
import pymysql as py
import pandas as pd
import json
import os

def limpar_dados(dados):
    """Converte valores NaN para None antes de inserir no banco de dados."""
    return {chave: None if pd.isna(valor) else valor for chave, valor in dados.items()}

def salvar_lead_no_db(dados):
    """Salva os dados de um lead no banco de dados."""
    conexao = py.connect(**db_config)
    cursor = conexao.cursor()

    try:
        # Verificar se o CPF já existe
        sql_verificar = "SELECT id FROM leads WHERE stCPF = %s"
        cursor.execute(sql_verificar, (dados['stCPF'],))
        lead_existente = cursor.fetchone()

        if lead_existente:
            lead_id = lead_existente[0]
        else:
            # Limpar dados para remover NaN
            dados_limpos = limpar_dados(dados)

            # Inserir novo lead
            sql_inserir_lead = """
                INSERT INTO leads (stCPF, stRg, dtBirth, stName, stEmail, stCity, stUF, stAddress, 
                                  stMothersName, stFathersName, blSanitized, dtSanitized, stCompany, 
                                  stOrgan, dcSalary, blBlock, dtReleasedIn, stNumber, stDistrict, 
                                  stPosition, stZipCode, isATaker, blBlackList)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(sql_inserir_lead, (
                dados_limpos['stCPF'], dados_limpos.get('stRg'), dados_limpos.get('dtBirth'), dados_limpos.get('stName'), 
                dados_limpos.get('stEmail'), dados_limpos.get('stCity'), dados_limpos.get('stUF'), dados_limpos.get('stAddress'), 
                dados_limpos.get('stMothersName'), dados_limpos.get('stFathersName'), dados_limpos.get('blSanitized'), 
                dados_limpos.get('dtSanitized'), dados_limpos.get('stCompany'), dados_limpos.get('stOrgan'), 
                dados_limpos.get('dcSalary'), dados_limpos.get('blBlock'), dados_limpos.get('dtReleasedIn'), 
                dados_limpos.get('stNumber'), dados_limpos.get('stDistrict'), dados_limpos.get('stPosition'), 
                dados_limpos.get('stZipCode'), dados_limpos.get('isATaker'), dados_limpos.get('blBlackList')
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
            'stName': valores[0] if pd.notna(valores[0]) else None,
            'stCPF': valores[1] if pd.notna(valores[1]) else None,
            'stRg': valores[2] if pd.notna(valores[2]) else None,
            'dtBirth': valores[3] if pd.notna(valores[3]) else None,
            'stEmail': valores[4] if pd.notna(valores[4]) else None,
            'stCity': valores[5] if pd.notna(valores[5]) else None,
            'stUF': valores[6] if pd.notna(valores[6]) else None,
            'stAddress': valores[7] if pd.notna(valores[7]) else None,
            'stMothersName': valores[8] if pd.notna(valores[8]) else None,
            'stFathersName': valores[9] if pd.notna(valores[9]) else None,
            'blSanitized': valores[10] if len(valores) > 10 and pd.notna(valores[10]) else None,
            'dtSanitized': valores[11] if len(valores) > 11 and pd.notna(valores[11]) else None,
            'stCompany': valores[12] if len(valores) > 12 and pd.notna(valores[12]) else None,
            'stOrgan': valores[13] if len(valores) > 13 and pd.notna(valores[13]) else None,
            'dcSalary': valores[14] if len(valores) > 14 and pd.notna(valores[14]) else None,
            'blBlock': valores[15] if len(valores) > 15 and pd.notna(valores[15]) else None,
            'dtReleasedIn': valores[16] if len(valores) > 16 and pd.notna(valores[16]) else None,
            'stNumber': valores[17] if len(valores) > 17 and pd.notna(valores[17]) else None,
            'stDistrict': valores[18] if len(valores) > 18 and pd.notna(valores[18]) else None,
            'stPosition': valores[19] if len(valores) > 19 and pd.notna(valores[19]) else None,
            'stZipCode': valores[20] if len(valores) > 20 and pd.notna(valores[20]) else None,
            'isATaker': valores[21] if len(valores) > 21 and pd.notna(valores[21]) else None, # só informa se tiver na tabela, coluna como tomador, se não tiver na coluna ele já cria
            'blBlackList': valores[22] if len(valores) > 22 and pd.notna(valores[22]) else None
        }
        
        lead_id = salvar_lead_no_db(dados_lead)
        
        if lead_id:
            print(f"Lead {lead_id} salvo com sucesso.")
        else:
            print(f"Erro ao salvar o lead {item[colunas[0]]}")    
        
def processar_arquivos(pasta_leads):
    arquivos_leads = [f for f in os.listdir(pasta_leads) if f.endswith('.csv')]
    
    for arquivo in arquivos_leads:
        print(f"lendo o arquivo de leads: {arquivo}")
        caminho_arquivo = os.path.join(pasta_leads, arquivo)
        
        try:
            df = pd.read_csv(caminho_arquivo, sep = ';', encoding = 'utf-8', engine = 'python')
            
            df = df.map(lambda x: None if pd.isna(x) else x)

            dados_json = df.to_dict(orient = "records")
            
        except Exception as e:
            print(f"erro ao ler a planilha {arquivo}: {e}")
        
        arquivo_processado_com_erro = False
        
        for item in dados_json:
            valores = list(item.values())
            
            if not valores[0]:
                print(f"erro: CPF ausente no registro {valores}")
                arquivo_processado_com_erro = True
                continue
            
            # Criar o dicionário de dados do lead
            dados_lead = {
                'stName': valores[0],
                'stCPF': valores[1],
                'stRg': valores[2],
                'dtBirth': valores[3],
                'stEmail': valores[4],
                'stCity': valores[5],
                'stUF': valores[6],
                'stAddress': valores[7],
                'stMothersName': valores[8],
                'stFathersName': valores[9],
                'blSanitized': valores[10] if len(valores) > 10 else None,
                'dtSanitized': valores[11] if len(valores) > 11 else None,
                'stCompany': valores[12] if len(valores) > 12 else None,
                'stOrgan': valores[13] if len(valores) > 13 else None,
                'dcSalary': valores[14] if len(valores) > 14 else None,
                'blBlock': valores[15] if len(valores) > 15 else None,
                'dtReleasedIn': valores[16] if len(valores) > 16 else None,
                'stNumber': valores[17] if len(valores) > 17 else None,
                'stDistrict': valores[18] if len(valores) > 18 else None,
                'stPosition': valores[19] if len(valores) > 19 else None,
                'stZipCode': valores[20] if len(valores) > 20 else None,
                'isATaker': valores[21] if len(valores) > 21 else None,
                'blBlackList': valores[22] if len(valores) > 22 else None
            }
            
            telefones_validos = [
                valores[i] for i in range(10, 20)
                if i < len(valores) and valores[i] and isinstance(valores[i], (str, int))
            ]
            
            lead_id = salvar_lead_no_db(dados_lead)  # Passar o dicionário ao invés da lista
            
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

print('Processo Iniciado')              
        
pasta_leads = r'.\leituraleads'
pasta_phones = r'.\leituraphone'

processar_arquivos(pasta_leads)
""""""
print('Processo Concluído')