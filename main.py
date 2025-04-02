from funcions import *
from flask import request, jsonify
from werkzeug.utils import secure_filename
from dbconfig import *
import logging
import os

app = create_app()

ALLOWED_EXTENSIONS = {'csv'} # extensão permitida

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS # se depois do . vai ta escrito csv

@app.route('/proccess', methods = ['POST'])
def processo_arquivo():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({"error": "Nenhum arquivo enviado"}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({"error": "Nome inválido"}), 400
        
        if not allowed_file(file.filename):
            return jsonify({"error": "Extensão não permitida (use um arquivo .csv)"}), 400
        
        filename = secure_filename(file.filename)
        
        # pasta onde dos arquivos CSV 
        pasta_leads = r'.\leituraleads'

        if not os.path.exists(pasta_leads):
            os.makedirs(pasta_leads)

        caminho_arquivo = os.path.join(pasta_leads, filename)
        file.save(caminho_arquivo)

        logging.info('Processo Iniciado')
        
        try:        
            processar_arquivos(pasta_leads)
            logging.info('Processo Concluído')
            
            return jsonify({"status": True, "message": "Processo Concluído"}), 200
        
        except Exception as e:
            logging.error(f"Erro ao processar arquivos: {str(e)}")
        
        return jsonify({f"error": "erro interno ao processar arquivos"}), 500

if __name__ == "__main__":
    app.run(host = '192.168.20.139', port = 8000, debug = True)