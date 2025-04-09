from dbconfig import db_config 
import pymysql as py
import pandas as pd
import datetime
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
    """Salva os dados de um lead no banco de dados, ignorando se já foi higienizado."""
    conexao = py.connect(**db_config)
    cursor = conexao.cursor()

    try:
        cpf_formatado = formatar_cpf(dados['stCPF'])
        
        if not cpf_formatado:
            logging.error("CPF inválido, não será salvo.")
            return None
        
        dados['stCPF'] = cpf_formatado
        
        # verificar se o CPF existe e se está higienizado 
        sql_verificar = "SELECT id, blSanitized FROM leads WHERE stCPF = %s"
        
        cursor.execute(sql_verificar, (cpf_formatado,))
        lead_existente = cursor.fetchone()

        if lead_existente:              
            lead_id, blSanitized = lead_existente
            
            if blSanitized:
                logging.info(f"Lead com o CPF {cpf_formatado} já foi higienizado, Ignorando...")                
                return None
            
            logging.info(f"Lead com CPF {cpf_formatado} já existe. ID: {lead_id}, Atualizando...")
            
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
    """Salva um número de telefone associado a um lead no banco de dados, se o número já existir pega e coloca no novo lead"""
    conexao = py.connect(**db_config)
    cursor = conexao.cursor()
    
    try:
        # filtra telefones válidos
        telefones_validos = [telefone for telefone in telefones if pd.notna(telefone)]
        
        for telefone in telefones_validos:
            # verifica se o telefone já existe na tabela
            sql_verificar = "SELECT id FROM phones WHERE stPhone = %s"
            cursor.execute(sql_verificar, (telefone,))
            resultado = cursor.fetchone()
            
            if resultado:
                # vai precisar atualizar para o novo lead_id
                sql_atualizar = "UPDATE phones SET lead_id = %s WHERE stPhone = %s"
                cursor.execute(sql_atualizar, (lead_id, telefone))
                logging.info(f"Telefone: {telefone} já existia e foi atualizado pro Lead ID {lead_id}.")
                
            else:
                sql_inserir = "INSERT INTO phones (stPhone, lead_id) VALUES (%s, %s)"
                cursor.execute(sql_inserir, (telefone, lead_id))
                logging.info(f"Telefone {telefone} inserido para o Lead ID {lead_id}.")
                
            conexao.commit()
            logging.info(f"Telefones verificados e atualizados/inseridos com sucesso para o Lead ID {lead_id}.")
            
            return True

    except Exception as e:
        logging.error(f"Erro ao atualizar/inserir o(s) telefone(s) para o Lead ID {lead_id}: {e}")
        
        return False
    
    finally:
        cursor.close()
        conexao.close()

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
            
            caminho_parquet = converter_csv_para_parquet(caminho_arquivo)
            logging.info(f"Arquivo convertido para parquet: {caminho_parquet}") 
            # pra ler o arquivo
            
            df = pd.read_parquet(caminho_parquet, engine = 'pyarrow')
            
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
                                                'dtBirth': tratar_dtBirth(item.get('dtBirth')),
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
                                                            #     'blBlock': item.get('blBlock'),
                                                            # 'dtReleasedIn': item.get('dtReleasedIn'),
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
                
                # ta salvando aqui
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
                novo_nome = caminho_parquet.replace('.parquet', '_erro.parquet')
                os.rename(caminho_parquet, novo_nome)
                logging.warning(f"Erro no arquivo {caminho_parquet}, renomeado para {novo_nome}")
            
            else:
                os.remove(caminho_parquet)
                logging.info(f"Arquivo {caminho_parquet} processado e apagado")

        except Exception as e:
            logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
            os.rename(caminho_arquivo, caminho_arquivo.replace('.csv', '_erro.csv'))
            
            # caso a conversão tenha criado o parquet mas o processamento falhou
            if os.path.exists(caminho_parquet):
                os.rename(caminho_parquet, caminho_parquet.replace('.parquet', '_erro.parquet'))
            
def converter_csv_para_parquet(caminho_csv):
    """converte um arquivo CSV para Parquet e retorna o novo caminho"""
    
    try:
        df = pd.read_csv(caminho_csv, sep = ';', encoding = 'utf-8', dtype = {'stCPF': str})
        
        # pra definir o novo nome do arquivo
        caminho_parquet = caminho_csv.replace('.csv', '.parquet')
        
        df.to_parquet(
                            caminho_parquet, 
                                engine = 'pyarrow', 
                                    index = False,
                                        compression = 'snappy'  # melhor compactação?
                                                                        )
        
        os.remove(caminho_csv) # trocar esse
        
        return caminho_parquet
        
    except Exception as e:
        logging.error(f"Falha na conversão de {caminho_csv} para Parquet: {e}")
        raise # propaga o erro pra tratamento externo
    
def formatar_cpf(cpf) -> str:
    # se é nulo ou NaN
    if cpf is None or (isinstance(cpf, float)):
        return None
    
    try:
        # se for float ou int, converte para inteiro primeiro 
        if isinstance(cpf, (float, int)):
            cpf = int(cpf)
        
        # converte pra string e filtra somente os dígitos
        cpf_str = ''.join([ch for ch in str(cpf) if ch.isdigit()])
        
        # se ainda tiver mais de 11, retorna none
        if len(cpf_str) > 11:
            logging.info(f"estão tendo {len(cpf_str)} dígitos no cpf que chega e retornando none")
            return None
        
        formatted_cpf = '{:0>11}'.format(cpf_str)
        
        return formatted_cpf
    
    except Exception as e:
        logging.error(f"Não consegui formatar o cpf, está assim {cpf_str.zfill(11)}, {e}")
        return None

def tratar_dtBirth(dtBirth):
    """formata o número da data de aniversário para o banco de dados"""
    
    if pd.isna(dtBirth):
        return None
    
    # se a data já for do tipo datetime, apenas formata
    if isinstance(dtBirth, datetime.date):
        return dtBirth.strftime("%Y-%m-%d")
    
    # tenta converter a string pra datetime assumindo o formato dd/mm/yyyy
    try:
        data_str = str(dtBirth).strip()
        
        formatos_possiveis = ["%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"]
        
        for formato in formatos_possiveis:
            try:
                dt = datetime.datetime.strptime(data_str, formato)
        
                return dt.strftime("%Y-%m-%d")
            
            except ValueError:
                continue
                
        logging.error(f"formato de data inválido: {data_str}")
        
        return None
    
    except Exception as e:
        logging.error(f"Erro ao processar a data: {dtBirth} - {str(e)}")
        return None