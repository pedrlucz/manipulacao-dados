from funcions import processar_arquivos

print('Processo Iniciado')              
        
pasta_leads = r'.\leituraleads'
pasta_phones = r'.\leituraphone'

processar_arquivos(pasta_leads, pasta_phones)

print('Processo Concluído')