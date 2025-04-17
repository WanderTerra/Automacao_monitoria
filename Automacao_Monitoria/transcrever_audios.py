import os
# Definir todas as variáveis de ambiente possíveis para evitar symlinks
os.environ["SPEECHBRAIN_DIALOG_STRATEGY"] = "copy"
os.environ["SPEECHBRAIN_LOCAL_FILE_STRATEGY"] = "copy"
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "0"
os.environ["HF_HUB_CACHE"] = os.path.join(os.path.expanduser("~"), ".cache", "huggingface", "no_symlinks")

# Hack para forçar SpeechBrain a usar cópia em vez de symlinks
import sys
import warnings
import json
from typing import Dict, Any, Optional

# Tentar configurar o SpeechBrain diretamente se possível
try:
    import speechbrain as sb
    # Sobrescreve a função de link que causa o erro
    import speechbrain.utils.fetching as fetching
    original_link_strategy = fetching.link_with_strategy
    
    def safe_link_strategy(src, dst, strategy):
        try:
            return original_link_strategy(src, dst, strategy)
        except OSError:
            print(f"Erro ao criar link simbólico. Usando cópia em vez disso.")
            import shutil
            shutil.copy(src, dst)
            return dst
            
    fetching.link_with_strategy = safe_link_strategy
    print("Configuração do SpeechBrain modificada para evitar erros de symlink")
except ImportError:
    print("SpeechBrain não encontrado, pulando configuração específica")

# Importações normais
import openai
import re
from dotenv import load_dotenv
from pathlib import Path
from openai import OpenAI

# Carrega as variáveis de ambiente do arquivo .env
dotenv_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path, override=True)  # override=True para garantir que nossas variáveis tenham prioridade

# Verifica se as chaves foram carregadas corretamente
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
HUGGINGFACE_TOKEN = os.getenv('HUGGINGFACE_TOKEN')

print(f"OPENAI_API_KEY configurada: {'Sim' if OPENAI_API_KEY else 'Não'}")
print(f"HUGGINGFACE_TOKEN configurado: {'Sim' if HUGGINGFACE_TOKEN else 'Não'}")

# Inicializa a API do Whisper da OpenAI (para transcrição)
openai.api_key = OPENAI_API_KEY

# ─── CONFIGURAÇÃO DO CLIENTE OPENAI PARA AVALIAÇÃO ────────────────────────────────────
_CLIENT: Optional[OpenAI] = None

def _get_client() -> OpenAI:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = OpenAI(
            api_key=OPENAI_API_KEY  # Usa a mesma chave já configurada para o projeto
        )
    return _CLIENT

# ─── PROMPT‑TEMPLATE PARA AVALIAÇÃO DE LIGAÇÕES ────────────────────────────────────────
SYSTEM_PROMPT = """
Você é o Monitor GPT, auditor de Qualidade das ligações da carteira Águas Guariroba
na Portes Advogados.

Sua missão:
1. Receber a transcrição bruta da chamada (português).
2. Avaliar cada item do CHECKLIST DE MONITORIA e das REGRAS DE CONFORMIDADE abaixo.
3. Para cada item, atribuir:
   • Conforme        → soma o peso
   • Não Conforme    → 0 ponto
   • Não se aplica   → 0 ponto
4. Calcular:
   – pontuacao_total       = soma dos pesos conforme
   – pontuacao_percentual  = (pontuacao_total / 10) * 100
5. Caso ocorra Falha Crítica (ofensa, vazamento de dados sensíveis ou transferência
   sem aviso), zere a nota final.
6. Responder EXCLUSIVAMENTE com um JSON no formato especificado em «MODELO DE SAÍDA».

CHECKLIST DE MONITORIA  (pesos)
- Abordagem
  • Atendeu o cliente prontamente?........................................(0.25)
- Segurança
  • Conduziu o atendimento com segurança, sem informações falsas?.........(0.50)
- Fraseologia de Momento e Retorno
  • Explicou motivo de ausência/transferência?............................(0.40)
- Comunicação
  • Tom de voz adequado, linguagem clara (pode ser informal), sem gírias?.....................(0.50)
- Cordialidade
  • Tratou o cliente com respeito, sem comentários impróprios?............(0.40)
- Empatia
  • Demonstrou empatia genuína?...........................................(0.40)
- Escuta Ativa
  • Ouviu sem interromper, retomando pontos? (o cliente pode interromper)..............................(0.40)
- Clareza & Objetividade
  • Explicações diretas, sem rodeios?.....................................(0.40)
- Oferta de Solução & Condições
  • Apresentou valores, descontos e opções corretamente? (somente se o cliente permitir)..................(0.40)
- Confirmação de Aceite
  • Confirmou negociação com "sim, aceito/confirmo"? (somente se fechou o acordo)......................(0.40)
- Reforço de Prazo & Condições
  • Reforçou data‑limite e perda de desconto? (somente se fechou o acordo).............................(0.40)
- Encerramento
  • Perguntou "Posso ajudar em algo mais?" e agradeceu? (somente se fechou o acordo)...................(0.40)


REGRAS DE CONFORMIDADE DO SCRIPT ÁGUAS GUARIOBA
✔ Identificar‑se com NOME + "Portes Advogados assessoria jurídica das Águas Guariroba"
✔ Confirmar nome/CPF e endereço antes da negociação
✔ Ofertar valor total, valor com desconto, entrada e parcelas ≥ R$ 20,00
✔ Perguntar se o número tem WhatsApp antes de enviar boleto
✔ Reforçar: "pagamento até X às 18h ou perderá o desconto"

MODELO DE SAÍDA
{
  "id_chamada": "...",
  "avaliador": "MonitorGPT",
  "itens": {
    "Abordagem": {
      "Atendeu prontamente": {
        "status": "Conforme|Não Conforme|N/A",
        "peso": 0.25,
        "observacao": "texto livre curto"
      }
    },
    ...
    "Falha Critica": {
      "Sem falha crítica": {
        "status": "Conforme|Não Conforme",
        "peso": 0
      }
    }
  },
  "pontuacao_total": 0‑10,
  "pontuacao_percentual": 0‑100
}
Não adicione nada fora desse JSON.
"""

# Inicializa o pipeline de Diarização do Pyannote.audio com tratamento de erros
try:
    from pyannote.audio import Pipeline
    pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization", use_auth_token=HUGGINGFACE_TOKEN)
    print("Pipeline de diarização inicializado com sucesso!")
except Exception as e:
    print(f"Erro ao inicializar pipeline de diarização: {e}")
    print("Tentando solução alternativa...")
    try:
        # Tenta uma abordagem alternativa sem usar SpeechBrain diretamente
        # Este é um fallback caso o método principal falhe
        import tempfile
        import torch
        
        from pyannote.audio.pipelines.utils import get_model
        from pyannote.audio.pipelines.speaker_diarization import SpeakerDiarization
        
        # Copia manualmente o modelo necessário 
        os.makedirs(os.path.join(os.path.expanduser("~"), ".cache", "torch", "pyannote", "speechbrain"), exist_ok=True)
        
        pipeline = SpeakerDiarization(segmentation="pyannote/segmentation")
        print("Pipeline alternativo inicializado!")
    except Exception as e2:
        print(f"Erro na solução alternativa: {e2}")
        print("AVISO: A diarização não funcionará. O script continuará apenas com transcrição.")
        pipeline = None

def parse_vtt(vtt_text):
    """
    Analisa o texto VTT e retorna uma lista de segmentos.
    Cada segmento é um dicionário com 'start', 'end' (em segundos) e 'text'.
    """
    segments = []
    # Expressão regular para capturar linhas de tempo no formato "HH:MM:SS.mmm --> HH:MM:SS.mmm"
    time_pattern = re.compile(r'(\d{2}:\d{2}:\d{2}\.\d{3}) --> (\d{2}:\d{2}:\d{2}\.\d{3})')
    lines = vtt_text.splitlines()
    idx = 0
    while idx < len(lines):
        line = lines[idx].strip()
        match = time_pattern.match(line)
        if match:
            start_str, end_str = match.groups()
            # Converte horário para segundos
            start = sum(float(x) * 60 ** i for i, x in enumerate(reversed(start_str.split(":"))))
            end = sum(float(x) * 60 ** i for i, x in enumerate(reversed(end_str.split(":"))))
            text_lines = []
            idx += 1
            # Junta as linhas seguintes até uma linha em branco
            while idx < len(lines) and lines[idx].strip() != "":
                text_lines.append(lines[idx].strip())
                idx += 1
            text = " ".join(text_lines)
            segments.append({"start": start, "end": end, "text": text})
        else:
            idx += 1
    return segments

def assign_speaker_to_segment(segment, diarization):
    """
    Para cada segmento de transcrição, verifica qual falante (do Pyannote) tem maior sobreposição.
    Retorna o rótulo do falante.
    """
    seg_start = segment["start"]
    seg_end = segment["end"]
    max_overlap = 0.0
    speaker_assigned = "Desconhecido"
    
    # Itera pelos segmentos de diarização; cada item tem (segment, _, speaker)
    for d_segment, _, speaker in diarization.itertracks(yield_label=True):
        # Calcula a sobreposição em segundos
        overlap = max(0, min(seg_end, d_segment.end) - max(seg_start, d_segment.start))
        if overlap > max_overlap:
            max_overlap = overlap
            speaker_assigned = speaker
    return speaker_assigned

def merge_transcript_and_diarization(vtt_text, diarization):
    """
    Mescla a transcrição (VTT) com os segmentos de diarização.
    Retorna um texto final onde cada segmento é rotulado com o falante.
    """
    segments = parse_vtt(vtt_text)
    final_lines = []
    for seg in segments:
        speaker = assign_speaker_to_segment(seg, diarization)
        # Cria uma linha formatada para o segmento:
        # [hh:mm:ss - hh:mm:ss] Falante: texto
        start_time = seg["start"]
        end_time = seg["end"]
        # Formata os tempos em hh:mm:ss
        def format_time(s):
            hrs = int(s // 3600)
            mins = int((s % 3600) // 60)
            secs = s % 60
            return f"{hrs:02d}:{mins:02d}:{secs:05.2f}"
        time_str = f"[{format_time(start_time)} - {format_time(end_time)}]"
        final_lines.append(f"{time_str} {speaker}: {seg['text']}")
    return "\n".join(final_lines)

def classificar_falantes_com_gpt(texto_transcricao):
    """
    Usa o modelo GPT-4.1-mini para identificar os falantes como 'Cliente' ou 'Agente'
    em uma transcrição de áudio.
    
    Args:
        texto_transcricao (str): O texto transcrito do áudio
        
    Returns:
        str: Texto da transcrição com os falantes identificados como Cliente ou Agente
    """
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        # Instrução clara para o modelo
        prompt = f"""
        Analise esta transcrição de uma ligação de cobrança e identifique quem está falando em cada momento.
        
        Regras para identificar os falantes:
        - O Cliente geralmente inicia com "Alô", pergunta "quem é" ou "quem fala", e responde às perguntas
        - O Agente geralmente se apresenta, dá bom dia, menciona a empresa, explica sobre débitos/cobranças
        - O Agente conduz a conversa fazendo perguntas sobre pagamentos
        - O Cliente geralmente responde às perguntas do agente
        
        Formato da transcrição original:
        [TIMESTAMP] SPEAKER_ID: texto da fala
        
        Substitua SPEAKER_ID por "Cliente" ou "Agente" baseado no contexto da conversa.
        Mantenha exatamente o mesmo texto e formato, mudando apenas a identificação do falante.
        
        Transcrição:
        {texto_transcricao}
        """
        
        response = client.chat.completions.create(
            model="gpt-4.1-mini",  # Usando o modelo gpt-4.1-mini
            messages=[
                {"role": "system", "content": "Você é um assistente especializado em identificar falantes em transcrições de ligações de cobrança."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,  # Temperatura baixa para respostas mais consistentes
            max_tokens=4096
        )
        
        return response.choices[0].message.content
    except Exception as e:
        print(f"Erro ao classificar falantes com GPT-4.1-mini: {e}")
        return texto_transcricao

def process_audio_file(caminho_audio):
    print(f"Transcrevendo com gpt-4o-transcribe: {caminho_audio}...")
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        with open(caminho_audio, 'rb') as audio_file:
            transcription_response = client.audio.transcriptions.create(
                model="gpt-4o-transcribe",
                file=audio_file,
                response_format="text"
            )
        # Se a resposta for um objeto Transcription, extrai o texto
        if hasattr(transcription_response, 'text'):
            text = transcription_response.text
        elif isinstance(transcription_response, str):
            text = transcription_response
        else:
            print(f"Formato de resposta desconhecido: {type(transcription_response)}")
            print(f"Conteúdo: {transcription_response}")
            return None
        print("Transcrição concluída!")
        return text
    except Exception as e:
        print(f"Erro na transcrição: {e}")
        return None

def process_audio_folder(pasta):
    extensoes_audio = ['.mp3', '.wav', '.m4a', '.ogg', '.flac']
    arquivos = [f for f in os.listdir(pasta) if os.path.splitext(f)[1].lower() in extensoes_audio]
    if not arquivos:
        print('Nenhum arquivo de áudio encontrado na pasta.')
        return
    pasta_audios_transcritos = os.path.join(pasta, 'Audios_transcritos')
    pasta_transcricoes = os.path.join(pasta, 'Transcrições_aguas')
    pasta_erros = os.path.join(pasta, 'Audios_erros')
    os.makedirs(pasta_audios_transcritos, exist_ok=True)
    os.makedirs(pasta_transcricoes, exist_ok=True)
    os.makedirs(pasta_erros, exist_ok=True)
    print(f"Pasta para áudios processados: {pasta_audios_transcritos}")
    print(f"Pasta para transcrições: {pasta_transcricoes}")
    print(f"Pasta para áudios com erro: {pasta_erros}")
    for arquivo in arquivos:
        caminho_audio = os.path.join(pasta, arquivo)
        print(f"Processando: {arquivo}")
        final_text = process_audio_file(caminho_audio)
        if final_text:
            nome_txt = os.path.splitext(arquivo)[0] + '_diarizado.txt'
            caminho_txt = os.path.join(pasta_transcricoes, nome_txt)
            with open(caminho_txt, 'w', encoding='utf-8') as f:
                f.write(final_text)
            print(f"Transcrição salva em: {caminho_txt}")
            caminho_destino = os.path.join(pasta_audios_transcritos, arquivo)
            try:
                import shutil
                shutil.move(caminho_audio, caminho_destino)
                print(f"Arquivo de áudio movido para: {caminho_destino}")
            except Exception as e:
                print(f"Erro ao mover o arquivo de áudio: {e}")
        else:
            caminho_erro = os.path.join(pasta_erros, arquivo)
            try:
                import shutil
                shutil.move(caminho_audio, caminho_erro)
                print(f"Falha ao processar {arquivo} - Arquivo movido para pasta de erros: {caminho_erro}")
                log_path = os.path.join(pasta_erros, f"{os.path.splitext(arquivo)[0]}_erro.txt")
                with open(log_path, 'w', encoding='utf-8') as log_file:
                    log_file.write(f"Erro ao processar o arquivo {arquivo}\n")
                    log_file.write(f"Data/hora: {format_time_now()}\n")
                    log_file.write(f"Falha na transcrição ou identificação de falantes")
            except Exception as move_error:
                print(f"Erro ao mover o arquivo com falha: {move_error}")

def format_time_now():
    """Retorna a data e hora atual formatadas"""
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def process_transcription_folder(pasta_transcricoes):
    """
    Processa as transcrições na pasta de transcrições, avalia-as e move para pasta de avaliadas.
    
    Args:
        pasta_transcricoes (str): Caminho da pasta com as transcrições
    """
    # Verificando se a pasta existe
    if not os.path.exists(pasta_transcricoes):
        print(f"Pasta de transcrições não encontrada: {pasta_transcricoes}")
        return
    
    # Criar pasta para transcrições avaliadas e pasta de erros
    pasta_transcricoes_avaliadas = os.path.join(pasta_transcricoes, 'Transcrições_avaliadas')
    pasta_transcricoes_erros = os.path.join(pasta_transcricoes, 'Transcrições_erros')
    
    os.makedirs(pasta_transcricoes_avaliadas, exist_ok=True)
    os.makedirs(pasta_transcricoes_erros, exist_ok=True)
    
    print(f"Pasta para transcrições avaliadas: {pasta_transcricoes_avaliadas}")
    print(f"Pasta para transcrições com erro: {pasta_transcricoes_erros}")
    
    # Obter todas as transcrições (arquivos .txt)
    arquivos_txt = [f for f in os.listdir(pasta_transcricoes) if f.endswith('.txt') and os.path.isfile(os.path.join(pasta_transcricoes, f))]
    
    if not arquivos_txt:
        print("Nenhuma transcrição encontrada para avaliação.")
        return
    
    print(f"Encontradas {len(arquivos_txt)} transcrições para avaliar.")
    
    for arquivo in arquivos_txt:
        caminho_transcricao = os.path.join(pasta_transcricoes, arquivo)
        id_chamada = os.path.splitext(arquivo)[0]  # Usa o nome do arquivo sem extensão como ID
        
        print(f"Avaliando transcrição: {arquivo}")
        
        try:
            # Ler o conteúdo da transcrição
            with open(caminho_transcricao, 'r', encoding='utf-8') as f:
                conteudo_transcricao = f.read()
            
            # Avaliar a transcrição
            avaliacao = avaliar_ligacao(conteudo_transcricao, id_chamada=id_chamada)
            
            # Criar nome para o arquivo de avaliação
            nome_avaliacao = f"{id_chamada}_avaliacao.json"
            caminho_avaliacao = os.path.join(pasta_transcricoes_avaliadas, nome_avaliacao)
            
            # Salvar a avaliação em formato JSON
            with open(caminho_avaliacao, 'w', encoding='utf-8') as f:
                json.dump(avaliacao, f, ensure_ascii=False, indent=2)
            
            print(f"Avaliação salva em: {caminho_avaliacao}")
            
            # Mover a transcrição avaliada
            caminho_destino = os.path.join(pasta_transcricoes_avaliadas, arquivo)
            import shutil
            shutil.copy2(caminho_transcricao, caminho_destino)  # Copia preservando metadados
            os.remove(caminho_transcricao)  # Remove o arquivo original
            print(f"Transcrição movida para: {caminho_destino}")
            
            # Criar um relatório de resumo em texto
            nota = avaliacao.get('pontuacao_percentual', 0)
            status = "APROVADA" if nota >= 70 else "REPROVADA"
            
            nome_resumo = f"{id_chamada}_resumo.txt"
            caminho_resumo = os.path.join(pasta_transcricoes_avaliadas, nome_resumo)
            
            with open(caminho_resumo, 'w', encoding='utf-8') as f:
                f.write(f"Avaliação da ligação: {id_chamada}\n")
                f.write(f"Status: {status}\n")
                f.write(f"Pontuação: {nota:.2f}%\n\n")
                f.write("Itens avaliados:\n")
                
                # Percorrer os itens de avaliação do JSON
                for categoria, itens in avaliacao.get('itens', {}).items():
                    f.write(f"\n{categoria}:\n")
                    for item_nome, item_info in itens.items():
                        status = item_info.get('status', 'N/A')
                        peso = item_info.get('peso', 0)
                        obs = item_info.get('observacao', '')
                        f.write(f"  • {item_nome}: {status} ({peso}) - {obs}\n")
            
            print(f"Resumo salvo em: {caminho_resumo}")
            
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")
            
            # Mover transcrição para pasta de erros
            try:
                import shutil
                caminho_destino_erro = os.path.join(pasta_transcricoes_erros, arquivo)
                shutil.copy2(caminho_transcricao, caminho_destino_erro)
                os.remove(caminho_transcricao)  # Remove o arquivo original
                print(f"Transcrição movida para pasta de erros: {caminho_destino_erro}")
                
                # Criar um arquivo de log explicando o erro
                log_path = os.path.join(pasta_transcricoes_erros, f"{id_chamada}_erro.txt")
                with open(log_path, 'w', encoding='utf-8') as log_file:
                    log_file.write(f"Erro ao avaliar a transcrição {arquivo}\n")
                    log_file.write(f"Data/hora: {format_time_now()}\n")
                    log_file.write(f"Erro: {str(e)}")
                print(f"Log de erro criado em: {log_path}")
            except Exception as move_error:
                print(f"Erro ao mover a transcrição com falha: {move_error}")

# ─── FUNÇÃO PARA AVALIAR LIGAÇÕES ──────────────────────────────────────────────
def avaliar_ligacao(transcricao: str, *, 
                    id_chamada: str = "chamada‑sem‑id") -> Dict[str, Any]:
    """
    Envia a transcrição ao modelo GPT-4.1-nano e devolve o JSON de avaliação.

    :param transcricao: Texto completo da ligação (turnos não precisam estar
                        separados, mas ajuda manter "Agente: / Cliente:").
    :param id_chamada:  Identificador que será inserido no campo `"id_chamada"`.
    :return:           dicionário Python com o resultado.
    :raises RuntimeError: se a resposta não for JSON válido.
    """
    client = _get_client()

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        # O id_chamada é passado como anotação no usuário para o modelo preencher
        {"role": "user",
         "content": f"ID_CHAMADA={id_chamada}\n\nTRANSCRICAO:\n{transcricao}"}
    ]

    print(f"Avaliando ligação: {id_chamada}")
    response = client.chat.completions.create(
        model="gpt-4.1-nano",  
        messages=messages,
        temperature=0.0,      # queremos avaliação consistente
        max_tokens=1024       # ajuste conforme necessidade
    )

    # O modelo deve responder TODO o JSON num único bloco
    assistant_content = response.choices[0].message.content.strip()

    try:
        result = json.loads(assistant_content)
        print(f"Avaliação concluída para ligação: {id_chamada}")
        return result
    except json.JSONDecodeError as e:
        error_msg = f"Resposta não é JSON válido para ligação {id_chamada}"
        print(f"ERRO: {error_msg}")
        raise RuntimeError(error_msg) from e

if __name__ == '__main__':
    pasta_audios = r'C:\Users\wanderley.terra\Documents\Audios_monitoria'
    
    # Processar os áudios primeiro
    process_audio_folder(pasta_audios)
    
    # Depois processar as transcrições para avaliação
    pasta_transcricoes = os.path.join(pasta_audios, 'Transcrições_aguas')
    process_transcription_folder(pasta_transcricoes)
    
    print("Processamento completo!")
