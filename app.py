from flask import Flask, jsonify
import json

import json
import pandas as pd
import zipfile
from io import BytesIO
from ftplib import FTP
import shutil  
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

def process_lines(txt_file):
    valid_lines = []
    error_lines = []

    for idx, line in enumerate(txt_file):
        try:
            line = line.decode('latin1').strip()
            cols = line.split('|')

            if len(cols) == 19:
                valid_lines.append(cols)
            else:
                error_lines.append((idx + 1, line))
        
        except Exception as e:
            error_lines.append((idx + 1, line))
            print(f"Erro ao processar a linha {idx + 1}: {e}")

    return valid_lines, error_lines


def download_and_process_data():
    try:
        ftp = FTP('ftp.mtps.gov.br')
        ftp.login(user='anonymous', passwd='guest@example.com')
        ftp.cwd('portal/fiscalizacao/seguranca-e-saude-no-trabalho/caepi/')
        files = ftp.nlst()
        print("Arquivos disponíveis:", files)

        desired_file = 'tgg_export_caepi.zip'

        with BytesIO() as f:
            ftp.retrbinary(f'RETR {desired_file}', f.write)
            f.seek(0)
            
            # Salvando cópia de backup do arquivo original
            with open('output/tgg_export_caepi.zip', 'wb') as backup_file:
                shutil.copyfileobj(f, backup_file)

            with zipfile.ZipFile(f) as z:
                with z.open('tgg_export_caepi.txt') as txt_file:
                    valid_lines, error_lines = process_lines(txt_file)
                    df_valid = pd.DataFrame(valid_lines)

        if not df_valid.empty:
            df_valid.columns = df_valid.iloc[0]
            df_valid = df_valid[1:]

            # Converter DataFrame para JSON
            json_data = df_valid.to_json(orient='records')

            # Salvar JSON em um arquivo
            with open('output/tgg_export_caepi_valid.json', 'w', encoding='utf-8') as json_file:
                json_file.write(json_data)

            print(f"Arquivo JSON 'tgg_export_caepi_valid.json' criado com sucesso com {len(df_valid)} linhas válidas.")

        if error_lines:
            with open('output/error_log.txt', 'w', encoding='utf-8') as log_file:
                log_file.write("Linhas com erro:\n")
                for line_num, line_text in error_lines:
                    log_file.write(f"Linha {line_num}: {line_text}\n")
            print(f"Erros encontrados em {len(error_lines)} linhas. Verifique 'error_log.txt' para detalhes.")

    except Exception as e:
        print(f"Erro: {e}")

    finally:
        ftp.quit()

app = Flask(__name__)

JSON_FILE_PATH = 'output/tgg_export_caepi_valid.json'

@app.route('/ca/<ca_id>', methods=['GET'])
def get_ca_info(ca_id):
  try:
    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as json_file:
      data = json.load(json_file)

    ca_info = [ca for ca in data if str(ca.get('#NRRegistroCA')) == str(ca_id)]
    if ca_info:
      return jsonify(ca_info), 200
    else:
      return jsonify({"erro": f"CA {ca_id} não encontrado"}), 404
  except FileNotFoundError:
    download_and_process_data()
    return jsonify({"erro": "Lista de CA não disponível, mas estamos tentando processá-lo agora. Por favor, tente novamente em alguns minutos."}), 500
  except ValueError:
    return jsonify({"erro": "Formato de ID CA inválido"}), 400
  except Exception as e:
    return jsonify({"erro": "Ocorreu um erro inesperado. Por favor, tente novamente mais tarde."}), 500

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(download_and_process_data, 'cron', hour=23)
    scheduler.start()

if __name__ == '__main__':
    start_scheduler()
    app.run(debug=True)
