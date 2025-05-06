import os
import requests
import csv

LOGIN_URL = 'https://portesmarinho.vonixcc.com.br/login/signin'
DOWNLOAD_URL = 'https://portesmarinho.vonixcc.com.br/recordings/{}'
USERNAME = 'kayro'
PASSWORD = '@Kl.#306'
PASTA_DESTINO = r'C:\Users\wanderley.terra\Documents\Audios_monitoria'
CSV_CALL_IDS = 'call_ids.csv'

# Função para fazer login e retornar a sessão autenticada
def fazer_login():
    session = requests.Session()
    payload = {
        'username': USERNAME,
        'password': PASSWORD
    }
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': LOGIN_URL
    }
    resp = session.post(LOGIN_URL, data=payload, headers=headers, allow_redirects=True)
    if resp.ok and 'logout' in resp.text.lower():
        print('Login realizado com sucesso!')
        return session
    else:
        print('Falha no login! Verifique usuário/senha ou se há autenticação extra.')
        print('Status:', resp.status_code)
        print('Resposta:', resp.text[:200])
        return None

def ler_call_ids_do_csv(caminho_csv):
    call_ids = []
    with open(caminho_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            call_id = row.get('call_id')
            if call_id:
                call_ids.append(call_id)
    return call_ids

def baixar_audio_por_call_id(session, call_id, pasta_destino):
    url = DOWNLOAD_URL.format(call_id)
    try:
        resposta = session.get(url, stream=True)
        content_type = resposta.headers.get('Content-Type', '')
        if resposta.status_code == 200 and 'audio' in content_type:
            caminho_arquivo = os.path.join(pasta_destino, f"{call_id}.wav")
            with open(caminho_arquivo, "wb") as f:
                for chunk in resposta.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"Áudio salvo em: {caminho_arquivo}")
        else:
            print(f"Falha ao baixar áudio para call_id {call_id}: {resposta.status_code} - {content_type}")
            print(resposta.text[:200])
    except Exception as e:
        print(f"Erro ao baixar áudio para call_id {call_id}: {e}")

if __name__ == "__main__":
    if not os.path.exists(PASTA_DESTINO):
        os.makedirs(PASTA_DESTINO)
    if not os.path.exists(CSV_CALL_IDS):
        print(f"Arquivo CSV '{CSV_CALL_IDS}' não encontrado.")
    else:
        session = fazer_login()
        if session:
            call_ids = ler_call_ids_do_csv(CSV_CALL_IDS)
            if not call_ids:
                print("Nenhum call_id encontrado no arquivo CSV.")
            for call_id in call_ids:
                baixar_audio_por_call_id(session, call_id, PASTA_DESTINO)
