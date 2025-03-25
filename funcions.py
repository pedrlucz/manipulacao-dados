from dbconfig import db_config 
import pymysql as py
import pandas as pd
import logging
import os

# configuração do logging, pra incluir conteudo da msg, nivel da msg e hora
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

def limpar_dados(dados):
    """Converte valores NaN para None antes de inserir no banco de dados."""
    return {chave: None if pd.isna(valor) else valor for chave, valor in dados.items()}

def converter_para_bool(valor):
    """Converte valores para boolean."""
    
    if pd.isna(valor):
        return None
    
    if isinstance(valor, bool):
        return valor
    
    if isinstance(valor, str):
        # compara com essa lista de verdadeira
        return valor.lower() in ['true', '1', 't', 'y', 'yes', 'sim', 's']
    
    # se for int ou float, converte pra boolean
    if isinstance(valor, (int, float)):
        return bool(valor)
    
    return None

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
            # mais uma verificação pro telefone unique aqui
            lead_id = lead_existente[0]
            logging.info(f"Lead com CPF {dados['stCPF']} já existe. ID: {lead_id}, Atualizando...")
            # atualziar as novas informações
            
            atualizar_lead_db(dados, lead_id)
            
            # extrair telefones antes de chamar a função
            telefones = [
                            dados.get(f'telefone{i}') for i in range(1, 11) if dados.get(f'telefone{i}') is not None
                                                                                                                        ]

            atualizar_telefone_db(lead_id, telefones)
            
        else:
            # limpar dados para remover NaN
            dados_limpos = limpar_dados(dados)

            # inserir novo lead
            colunas = []
            valores = []
            
            for chave, valor in dados_limpos.items():
                if valor is not None:  # ignorar valores None
                    colunas.append(chave)
                    valores.append(valor)

            # montar a query dinamicamente
            sql_inserir_lead = f"""
                                        INSERT INTO leads ({', '.join(colunas)})
                                            VALUES ({', '.join(['%s'] * len(colunas))})
                                                                                            """

            cursor.execute(sql_inserir_lead, valores)
            conexao.commit()
            lead_id = cursor.lastrowid # recupera o id do lead que acabou de ser inserido
            logging.info(f"Novo lead inserido com ID: {lead_id}")

        return lead_id

    except Exception as e:
        logging.error(f"Erro ao salvar lead no banco: {e}")
        return None

    finally:
        cursor.close()
        conexao.close()

def salvar_telefone_no_db(lead_id, telefones):
    """Salva um número de telefone associado a um lead no banco de dados."""
    
    # verifica se a lista de telefones não está vazia
    if telefones:
        telefones_validos = [telefone for telefone in telefones if pd.notna(telefone)]

        if telefones_validos:
            conexao = py.connect(**db_config)
            cursor = conexao.cursor()

            try:
                for telefone in telefones_validos:
                    
                    sql_inserir_telefone = """
                                                    INSERT INTO phones (stPhone, lead_id)
                                                        VALUES (%s, %s)
                                                                                                """
                                                                                                
                    cursor.execute(sql_inserir_telefone, (telefone, lead_id))
                    conexao.commit()

            except Exception as e:
                logging.error(f"Erro ao salvar telefone no banco: {e}")

            finally:
                cursor.close()
                conexao.close()
        else:
            logging.warning(f"Nenhum telefone válido para salvar para o lead ID {lead_id}")

def atualizar_lead_db(dados, lead_id):
    """Atualiza os dados de um lead existente no banco de dados"""
    
    conexao_db = py.connect(**db_config)
    cursor = conexao_db.cursor()
    
    try:

        # limpar os dados para remover NaN
        dados_limpos = limpar_dados(dados)
        
        # mostrar a query dinamicamente apenas para os campos que possuem valor
        coluna_valores = []
        valores = []
        
        for chave, valor in dados_limpos.items():
            if valor is not None: # ignorar valores nones
                coluna_valores.append(f"{chave} = %s")
                valores.append(valor)
                
        if not coluna_valores:
            logging.info(f"Nenhuma atualização necessária para o lead {lead_id}")
            
            return False
        
        valores.append(lead_id) # adicionar o id ao final da cláusula WHERE
        
        sql_update = f"""
            UPDATE leads
            SET {', '.join(coluna_valores)}
            WHERE id = %s
        """
        
        cursor.execute(sql_update, valores)
        conexao_db.commit()
        logging.info(f"Lead ID {lead_id} atualizado com sucesso.")
        
        return True
        
    except Exception as e:
        logging.error(f"Erro ao atualizar lead ID {lead_id}: {e}")
        return False
    
    finally:
        cursor.close()
        conexao_db.close()

def atualizar_telefone_db(lead_id, telefones):
    """Atualiza os telefones associados a um lead no banco de dados"""
    conexao_db = py.connect(**db_config)
    cursor = conexao_db.cursor()
    
    try:
        # limpar telefones inválidos
        telefones_validos = [telefone for telefone in telefones if pd.notna(telefone)]
        
        if not telefones_validos:
            logging.warning(f"Nenhum telefone válido pra atualizar pro LEAD ID: {lead_id}")
            return False
        
        # remove telefones antigos 
        cursor.execute("DELETE FROM phones WHERE lead_id = %s", (lead_id, ))
        conexao_db.commit()    

        # inserir novos telefones
        for telefone in telefones_validos:
            sql_inserir_telefone = """
                INSERT INTO phones (stPhone, lead_id)
                VALUE (%s, %s) 
            """
            
            cursor.execute(sql_inserir_telefone, (telefone, lead_id))
            
        conexao_db.commit()
        logging.info(f"Telefones do lead ID {lead_id} atualizados com sucesso.")
        
        return True
    
    except Exception as e:
        logging.error(f"Erro ao atualizar telefones do lead ID {lead_id}: {e}")
        
        return False

    finally:
        cursor.close()
        conexao_db.close()

def processar_arquivos(pasta_leads):
    """Processa todos os arquivos CSV na pasta de leads."""
    
    # os.listdir para pegar filtrar só os que terminam com .csv
    arquivos_leads = [f for f in os.listdir(pasta_leads) if f.endswith('.csv')]

    for arquivo in arquivos_leads:
        # pra cada arquivo csv vai montar um path
        caminho_arquivo = os.path.join(pasta_leads, arquivo)
        logging.info(f"Processando arquivo: {arquivo}")

        try:
            # pra ler o arquivo
            df = pd.read_csv(caminho_arquivo, sep = ';', encoding = 'utf-8')
            df = df.map(lambda x: None if pd.isna(x) else x)  # converte NaN para None

            # verifica se as colunas opcionais existem
            colunas_opcionais = ['isATaker', 'blBlackList', 'blSanitized']
            
            for coluna in colunas_opcionais:
                if coluna not in df.columns:
                    df[coluna] = None  # adiciona a coluna com valores None se não existir

            # converter campos booleanos
            if 'isATaker' in df.columns:
                df['isATaker'] = df['isATaker'].apply(converter_para_bool)
                
            if 'blBlackList' in df.columns:
                df['blBlackList'] = df['blBlackList'].apply(converter_para_bool)
                
            if 'blSanitized' in df.columns:
                df['blSanitized'] = df['blSanitized'].apply(converter_para_bool)

            # converte o dataframe pra uma lista de dicionário(um por registro), pra facilitar
            dados_json = df.to_dict(orient = 'records')

            arquivo_processado_com_erro = False

            for item in dados_json:
                # criar o dicionário de dados do lead
                dados_lead = {
                                    'stName': item.get('stName'),
                                        'stCPF': item.get('stCPF'),
                                            'stRg': item.get('stRg'),
                                                'dtBirth': item.get('dtBirth'),
                                                    'stEmail': item.get('stEmail'),
                                                        'stCity': item.get('stCity'),
                                                            'stUF': item.get('stUF'),
                                                                'stAddress': item.get('stAddress'),
                                                                    'stMothersName': item.get('stMothersName'),
                                                                        'stFathersName': item.get('stFathersName'),
                                                                            'blSanitized': item.get('blSanitized'),
                                                                                'dtSanitized': item.get('dtSanitized'),
                                                                            'stCompany': item.get('stCompany'),
                                                                        'stOrgan': item.get('stOrgan'),
                                                                    'dcSalary': item.get('dcSalary'),
                                                                'blBlock': item.get('blBlock'),
                                                            'dtReleasedIn': item.get('dtReleasedIn'),
                                                        'stNumber': item.get('stNumber'),
                                                    'stDistrict': item.get('stDistrict'),
                                                'stPosition': item.get('stPosition'),
                                            'stZipCode': item.get('stZipCode'),
                                        'isATaker': item.get('isATaker'),  # campo boolean
                                    'blBlackList': item.get('blBlackList')  # campo boolean
                                                                                                                                }

                # verificar se o CPF está presente
                if not dados_lead['stCPF']:
                    logging.error(f"Erro: CPF ausente no registro {item}")
                    arquivo_processado_com_erro = True
                    continue

                lead_id = salvar_lead_no_db(dados_lead)

                if not lead_id:
                    arquivo_processado_com_erro = True
                    continue

                # salvar telefones associados ao lead
                telefones_validos = [
                                            item.get(f'telefone{i}') for i in range(1, 11)  # para 10 telefones
                                                if item.get(f'telefone{i}') is not None
                                                                                                                            ]
                
                salvar_telefone_no_db(lead_id, telefones_validos)

            if arquivo_processado_com_erro == True:
                novo_nome = caminho_arquivo.replace('.csv', '_erro.csv')
                os.rename(caminho_arquivo, novo_nome)
                logging.warning(f"Erro no arquivo {arquivo}, renomeado para {novo_nome}")
            
            else:
                os.remove(caminho_arquivo)
                logging.info(f"Arquivo {arquivo} processado e apagado")

        except Exception as e:
            logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")