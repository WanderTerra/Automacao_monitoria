import os
import re
import argparse
from pathlib import Path

def identificar_falantes_transcrição(texto):
    """
    Classifica os falantes como "Cliente" ou "Conciliador" baseado em padrões na conversação.
    
    Regras:
    1. O Cliente geralmente inicia com "Alô"
    2. O Conciliador geralmente se apresenta, dá bom dia e chama o cliente pelo nome
    """
    linhas = texto.strip().split('\n')
    result = []
    speaker_ids = {}
    primeiro_alô_encontrado = False
    
    for linha in linhas:
        if not linha.strip():
            result.append(linha)
            continue
        
        # Localiza o formato padrão "[HH:MM:SS.MMM - HH:MM:SS.MMM] SPEAKER_ID: texto"
        match = re.match(r'(\[\d{2}:\d{2}:\d{2}\.\d{3} - \d{2}:\d{2}:\d{2}\.\d{3}\]) (SPEAKER_\d+|[^:]+): (.*)', linha)
        if match:
            timestamp, speaker, texto_fala = match.groups()
            
            # Verifica características da fala para identificar o falante
            if "alô" in texto_fala.lower() and not primeiro_alô_encontrado:
                speaker = "Cliente"
                primeiro_alô_encontrado = True
                speaker_ids[speaker] = speaker
            elif any(term in texto_fala.lower() for term in ["bom dia", "boa tarde", "boa noite"]) and \
                 any(term in texto_fala.lower() for term in ["me chamo", "meu nome é", "falo da"]):
                speaker = "Conciliador"
                speaker_ids[speaker] = speaker
            elif "quem fala" in texto_fala.lower():
                speaker = "Cliente"
                speaker_ids[speaker] = speaker
            elif any(term in texto_fala.lower() for term in ["débito", "desconto", "pagamento", "notificação", "assessoria", "cobrança"]):
                speaker = "Conciliador"
                speaker_ids[speaker] = speaker
            # Se não tivermos certeza, mas já identificamos este ID antes
            elif speaker in speaker_ids:
                speaker = speaker_ids[speaker]
            # Se não conseguimos identificar, mantemos o ID original
            
            nova_linha = f"{timestamp} {speaker}: {texto_fala}"
            result.append(nova_linha)
        else:
            result.append(linha)
    
    # Segunda passagem para identificar falantes restantes baseados no padrão da conversa
    if "Cliente" in speaker_ids.values() and "Conciliador" in speaker_ids.values():
        for i, linha in enumerate(result):
            if not linha.strip():
                continue
                
            match = re.match(r'(\[\d{2}:\d{2}:\d{2}\.\d{3} - \d{2}:\d{2}:\d{2}\.\d{3}\]) (SPEAKER_\d+|[^:]+): (.*)', linha)
            if match and match.group(2) not in ["Cliente", "Conciliador"]:
                # Analisa o contexto para identificar o falante
                # Se a linha anterior e posterior foram do mesmo tipo, este provavelmente é do outro tipo
                i_ant = i - 1
                i_pos = i + 1
                
                while i_ant >= 0 and not re.search(r'(Cliente|Conciliador)', result[i_ant]):
                    i_ant -= 1
                    
                while i_pos < len(result) and not re.search(r'(Cliente|Conciliador)', result[i_pos]):
                    i_pos += 1
                
                anterior = None if i_ant < 0 else re.search(r'(Cliente|Conciliador)', result[i_ant]).group(1)
                posterior = None if i_pos >= len(result) else re.search(r'(Cliente|Conciliador)', result[i_pos]).group(1)
                
                if anterior == posterior and anterior is not None:
                    # Se ambos são iguais, esta linha provavelmente é do outro falante
                    speaker = "Cliente" if anterior == "Conciliador" else "Conciliador"
                else:
                    # Caso contrário, usar heurística baseada no conteúdo
                    texto_fala = match.group(3).lower()
                    if any(p in texto_fala for p in ["obrigado", "tá bom", "pode ser", "nem lembrava"]):
                        speaker = "Cliente"
                    else:
                        speaker = "Conciliador"
                
                timestamp, _, texto_fala = match.groups()
                result[i] = f"{timestamp} {speaker}: {texto_fala}"
    
    return '\n'.join(result)

def processar_arquivo_transcricao(caminho_arquivo):
    """Processa um arquivo de transcrição para identificar corretamente os falantes."""
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as file:
            conteudo = file.read()
        
        conteudo_processado = identificar_falantes_transcrição(conteudo)
        
        # Salva o arquivo processado
        nome_base, ext = os.path.splitext(caminho_arquivo)
        caminho_saida = f"{nome_base}_identificado{ext}"
        with open(caminho_saida, 'w', encoding='utf-8') as file:
            file.write(conteudo_processado)
        
        print(f"Arquivo processado salvo em: {caminho_saida}")
        return True
    except Exception as e:
        print(f"Erro ao processar arquivo: {e}")
        return False

def processar_pasta(pasta):
    """Processa todos os arquivos de transcrição em uma pasta."""
    arquivos = [f for f in os.listdir(pasta) if f.endswith('.txt') and not f.endswith('_identificado.txt')]
    for arquivo in arquivos:
        caminho_arquivo = os.path.join(pasta, arquivo)
        print(f"Processando: {arquivo}")
        processar_arquivo_transcricao(caminho_arquivo)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Identifica falantes em transcrições")
    parser.add_argument("caminho", help="Caminho para o arquivo ou pasta de transcrições")
    args = parser.parse_args()
    
    caminho = args.caminho
    
    if os.path.isdir(caminho):
        processar_pasta(caminho)
    elif os.path.isfile(caminho):
        processar_arquivo_transcricao(caminho)
    else:
        print(f"Caminho não encontrado: {caminho}")