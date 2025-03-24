from funcions import processar_arquivos
import logging

# pasta onde dos arquivos CSV 
pasta_leads = r'.\leituraleads'

# logging pra gerir os logs
logging.info('Processo Iniciado')
processar_arquivos(pasta_leads)
logging.info('Processo Concluído')