import os
import time
import csv
import mysql.connector
from playwright.sync_api import sync_playwright

LOGIN_URL = 'https://portesmarinho.vonixcc.com.br/login/signin'
DOWNLOAD_URL = 'https://portesmarinho.vonixcc.com.br/recordings/{}'
USERNAME = 'kayro'
PASSWORD = '@Kl.#306'
PASTA_DESTINO = r'C:\Users\wanderley.terra\Documents\Audios_monitoria'

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

SQL_QUERY = '''
SELECT call_id, queue_id, start_time, answer_time, hangup_time, call_secs
FROM vonix.calls AS c
WHERE queue_id LIKE 'aguas%'
  AND queue_id NOT LIKE 'aguasguariroba%'
  AND status LIKE 'Completada%'
  AND start_time >= '2025-05-05 00:00:00'
  AND start_time < '2025-05-07 00:00:00'
  AND call_secs > 60
ORDER BY start_time DESC
'''

def buscar_call_ids_do_banco():
    call_ids = []
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        rows = cursor.fetchall()
        for row in rows:
            call_ids.append(row[0])
        cursor.close()
        conn.close()
        print(f"{len(call_ids)} call_ids encontrados no banco.")
    except Exception as e:
        print(f"Erro ao consultar banco: {e}")
    return call_ids

def baixar_audios_com_playwright(call_ids):
    if not os.path.exists(PASTA_DESTINO):
        os.makedirs(PASTA_DESTINO)
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
                caminho_arquivo = os.path.join(PASTA_DESTINO, nome_arquivo)
                download.save_as(caminho_arquivo)
                print(f'Áudio salvo: {caminho_arquivo}')
            except Exception as e:
                print(f'Falha ao baixar {call_id}: {e}')
        browser.close()

if __name__ == '__main__':
    call_ids = buscar_call_ids_do_banco()
    if not call_ids:
        print("Nenhum call_id encontrado na consulta SQL.")
    else:
        baixar_audios_com_playwright(call_ids)
