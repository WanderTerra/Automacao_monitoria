import os
import time
import csv
import mysql.connector
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta

# URLs e credenciais do site
LOGIN_URL = 'https://portesmarinho.vonixcc.com.br/login/signin'
DOWNLOAD_URL = 'https://portesmarinho.vonixcc.com.br/recordings/{}'
USERNAME = 'kayro'
PASSWORD = '@Kl.#306'

# Pasta base onde os áudios serão salvos
BASE_PASTA = r'C:\Users\wanderley.terra\Documents\Audios_monitoria'

# Configuração do banco de dados
DB_CONFIG = {
    'host': '10.100.10.57',
    'port': 3306,
    'user': 'user_automacao',
    'password': 'G5T82ZWMr',
    'database': 'vonix',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
}

# Configurações para as carteiras. Agora as pastas "Águas Guariroba" e "Vuon"
# ficam no mesmo nível dentro de BASE_PASTA.
CONFIG_CARTEIRAS = [
    {
        "nome": "aguas_guariroba",
        "sql_query": '''
SELECT call_id, queue_id, start_time, answer_time, hangup_time, call_secs
FROM vonix.calls AS c
WHERE queue_id LIKE '%aguas%'
    AND queue_id NOT LIKE 'aguasguariroba%'
    AND status LIKE 'Completada%'
    AND start_time >= '2025-07-30 00:00:00'
    AND call_secs > 60
ORDER BY start_time DESC
''',
        "pasta_destino": os.path.join(BASE_PASTA, "Águas Guariroba")
    },
    {
        "nome": "vuon",
        "sql_query": '''
SELECT call_id, queue_id, start_time, answer_time, hangup_time, call_secs
FROM vonix.calls AS c
WHERE queue_id LIKE '%vuon%'
    AND status LIKE 'Completada%'
    AND start_time >= '2025-07-30 00:00:00'
    AND call_secs > 60
ORDER BY start_time DESC
''',
        "pasta_destino": os.path.join(BASE_PASTA, "Vuon")
    },
    {
        "nome": "unimed",
        "sql_query": '''
SELECT call_id, queue_id, start_time, answer_time, hangup_time, call_secs
FROM vonix.calls AS c
WHERE queue_id LIKE '%unimed%'
    AND status LIKE 'Completada%'
    AND start_time >= '2025-07-30 00:00:00'
    AND call_secs > 60
ORDER BY start_time DESC
''',
        "pasta_destino": os.path.join(BASE_PASTA, "Unimed")
    }
]

def buscar_call_ids_do_banco(sql_query):
    call_ids = []
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        for row in rows:
            call_ids.append(row[0])
        cursor.close()
        conn.close()
        print(f"{len(call_ids)} call_ids encontrados no banco.")
    except Exception as e:
        print(f"Erro ao consultar banco: {e}")
    return call_ids

def salvar_mapeamento_call_ids(mapeamento, pasta_destino):
    arquivo_mapeamento = os.path.join(pasta_destino, 'mapeamento_call_ids.csv')
    with open(arquivo_mapeamento, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['nome_arquivo', 'call_id'])
        for nome_arquivo, call_id in mapeamento.items():
            writer.writerow([nome_arquivo, call_id])
    print(f"Mapeamento salvo em: {arquivo_mapeamento}")

def baixar_audios_com_playwright(call_ids, pasta_destino):
    # Cria a pasta de destino, se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    
    mapeamento = {}    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        # Login
        page.goto(LOGIN_URL)
        page.fill('#username', USERNAME)
        page.fill('#password', PASSWORD)
        page.click('xpath=//*[@id="wrapper"]/div/form/dl/dd[3]/input')
        time.sleep(2)  # Aguarda 2 segundos após o login
        print('Login realizado com Playwright.')
        # Baixar cada áudio
        for call_id in call_ids:
            url = DOWNLOAD_URL.format(call_id)
            try:
                with page.expect_download() as download_info:
                    page.evaluate(f"window.location.href = '{url}'")
                download = download_info.value
                nome_arquivo = download.suggested_filename
                caminho_arquivo = os.path.join(pasta_destino, nome_arquivo)
                download.save_as(caminho_arquivo)
                mapeamento[nome_arquivo] = call_id
                print(f"Áudio salvo: {caminho_arquivo} (call_id: {call_id})")
            except Exception as e:
                print(f"Falha ao baixar {call_id}: {e}")
        browser.close()
    
    salvar_mapeamento_call_ids(mapeamento, pasta_destino)

if __name__ == '__main__':
    # Para cada carteira, busca os call_ids e baixa os áudios na pasta específica
    for config in CONFIG_CARTEIRAS:
        print(f"\nProcessando carteira: {config['nome']}")
        call_ids = buscar_call_ids_do_banco(config['sql_query'])
        if not call_ids:
            print(f"Nenhum call_id encontrado para a carteira {config['nome']}.")
        else:
            baixar_audios_com_playwright(call_ids, config['pasta_destino'])