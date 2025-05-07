import json
import os
import re
import sys
import time
import csv
import glob
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv
import openai
from openai import OpenAI
from pydub.utils import mediainfo

# Definir todas as variáveis de ambiente possíveis para evitar symlinks
os.environ["SPEECHBRAIN_DIALOG_STRATEGY"] = "copy"
os.environ["SPEECHBRAIN_LOCAL_FILE_STRATEGY"] = "copy"
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "0"
os.environ["HF_HUB_CACHE"] = os.path.join(os.path.expanduser("~"), ".cache", "huggingface", "no_symlinks")

# Hack para forçar SpeechBrain a usar cópia em vez de symlinks
try:
    import speechbrain as sb
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
    print("SpeechBrain não está instalado. Algumas funcionalidades podem não estar disponíveis.")


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

MAX_SEM_GSS = 9.60 #Trocar para 10 no prompt quando a pontuação do GSS for inserida
# ─── PROMPT‑TEMPLATE PARA AVALIAÇÃO DE LIGAÇÕES ────────────────────────────────────────
SYSTEM_PROMPT = f"""
Você é o Monitor GPT, auditor de Qualidade das ligações da carteira Águas Guariroba
na Portes Advogados.

Sua missão:
1. Receber a transcrição bruta da chamada (português).
2. Avaliar cada item do CHECKLIST DE MONITORIA e das REGRAS DE CONFORMIDADE abaixo.
3. Para cada item, atribuir:
   • Conforme        → soma o peso
   • Não Conforme    → 0 ponto
   • Não se aplica   → 0 ponto
   3.1 Se o requisito não ocorreu porque a situação não aconteceu
       (ex.: não houve acordo → itens de aceite/encerramento ficam N/A),
       marque "N/A" e NÃO penalize.
4. Calcular:
   – pontuacao_total       = soma dos pesos conforme
   – pontuacao_percentual  = (pontuacao_total / {MAX_SEM_GSS}) * 100
5. Caso ocorra Falha Crítica (ofensa, vazamento de dados sensíveis ou transferência
   sem aviso), zere a nota final.
6. Responder EXCLUSIVAMENTE com um JSON no formato especificado em «MODELO DE SAÍDA».

IMPORTANTE: Cada subitem do checklist DEVE ser um objeto/dicionário com as chaves "status", "peso" e "observacao" (quando aplicável). NUNCA retorne apenas uma string como valor do subitem. Siga exatamente o modelo abaixo.

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
- Confirmação de Aceite caso o cliente aceite a negociação
  • Confirmou negociação com "sim, aceito/confirmo"? ......................(0.40)
- Reforço de Prazo & Condições caso o cliente aceite a negociação
  • Reforçou data‑limite e perda de desconto? (somente se fechou o acordo).............................(0.40)
- Encerramento
  • Perguntou "Posso ajudar em algo mais?" e agradeceu? (somente se fechou o acordo)...................(0.40)

REGRAS DE CONFORMIDADE DO SCRIPT ÁGUAS GUARIOBA
✔ Identificar‑se com NOME + "Portes Advogados assessoria jurídica das Águas Guariroba"
✔ Confirmar nome ou CPF e endereço antes da negociação
✔ Ofertar valor total, valor com desconto, entrada e parcelas ≥ R$ 20,00
✔ Perguntar se o número tem WhatsApp antes de enviar boleto
✔ Reforçar: "pagamento até X às 18h ou perderá o desconto"

MODELO DE SAÍDA
{{
  "id_chamada": "...",
  "avaliador": "MonitorGPT",
  "itens": {{
    "Abordagem": {{
      "Atendeu prontamente": {{
        "status": "Conforme|Não Conforme",
        "peso": 0.25,
        "observacao": "texto livre curto"
      }}
    }},
    "Segurança": {{
      "Conduziu o atendimento com segurança, sem informações falsas": {{
        "status": "Conforme|Não Conforme",
        "peso": 0.5,
        "observacao": "texto livre curto"
      }}
    }},
    "Fraseologia de Momento e Retorno": {{
      "Explicou motivo de ausência/transferência": {{
        "status": "Conforme|Não Conforme|N/A",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Comunicação": {{
      "Tom de voz adequado, linguagem clara (pode ser informal), sem gírias": {{
        "status": "Conforme|Não Conforme",
        "peso": 0.5,
        "observacao": "texto livre curto"
      }}
    }},
    "Cordialidade": {{
      "Tratou o cliente com respeito, sem comentários impróprios": {{
        "status": "Conforme|Não Conforme",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Empatia": {{
      "Demonstrou empatia genuína": {{
        "status": "Conforme|Não Conforme",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Escuta Ativa": {{
      "Ouviu sem interromper, retomando pontos": {{
        "status": "Conforme|Não Conforme",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Clareza & Objetividade": {{
      "Explicações diretas, sem rodeios": {{
        "status": "Conforme|Não Conforme",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Oferta de Solução & Condições": {{
      "Apresentou valores, descontos e opções corretamente": {{
        "status": "Conforme|Não Conforme|N/A",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Confirmação de Aceite": {{
      "Confirmou negociação com 'sim, aceito/confirmo'": {{
        "status": "Conforme|Não Conforme|N/A",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Reforço de Prazo & Condições": {{
      "Reforçou data‑limite e perda de desconto": {{
        "status": "Conforme|Não Conforme|N/A",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Encerramento": {{
      "Perguntou 'Posso ajudar em algo mais?' e agradeceu": {{
        "status": "Conforme|Não Conforme|N/A",
        "peso": 0.4,
        "observacao": "texto livre curto"
      }}
    }},
    "Falha Critica": {{
      "Sem falha crítica": {{
        "status": "Conforme|Não Conforme",
        "peso": 0,
        "observacao": "texto livre curto"
      }}
    }}
  }},
  "pontuacao_total": 0-10,
  "pontuacao_percentual": 0-100
}}
NÃO retorne NENHUM valor de subitem como string simples. Siga exatamente o modelo acima.
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
        import tempfile
        import torch
        
        from pyannote.audio.pipelines.utils import get_model
        from pyannote.audio.pipelines.speaker_diarization import SpeakerDiarization
        
        os.makedirs(os.path.join(os.path.expanduser("~"), ".cache", "torch", "pyannote", "speechbrain"), exist_ok=True)
        
        pipeline = SpeakerDiarization(segmentation="pyannote/segmentation")
        print("Pipeline alternativo inicializado!")
    except Exception as e2:
        print(f"Erro na solução alternativa: {e2}")
        print("AVISO: A diarização não funcionará. O script continuará apenas com transcrição.")
        pipeline = None

def corrigir_portes_advogados(texto):
    """
    Corrige variações comuns de transcrição para 'Portes Advogados'.
    """
    padroes = [
        r'partes de advogados',
        r'porta de advogados',
        r'parte de advogados',
        r'portas de advogados',
        r'portas advogados',
        r'porta advogados',
        r'partes advogados',
        r'porta dos advogados',
        r'portas dos advogados',
        r'parte dos advogados',
        r'partes dos advogados',
        r'parte da advogados',
        r'portas da advogados',
        r'porta da advogados',
        r'porta advogada',
        r'portas advogadas',
        r'parte advogada',
        r'porta de advogado',
        r'portas de advogado',
        r'parte de advogado',
        r'pai dos advogados',
        r'porto advogados'
    ]
    for padrao in padroes:
        texto = re.sub(padrao, 'Portes Advogados', texto, flags=re.IGNORECASE)
    return texto

def parse_vtt(vtt_text):
    """
    Analisa o texto VTT e retorna uma lista de segmentos.
    Cada segmento é um dicionário com 'start', 'end' (em segundos) e 'text'.
    """
    segments = []
    time_pattern = re.compile(r'(\d{2}:\d{2}:\d{2}\.\d{3}) --> (\d{2}:\d{2}:\d{2}\.\d{3})')
    lines = vtt_text.splitlines()
    idx = 0
    while idx < len(lines):
        line = lines[idx].strip()
        match = time_pattern.match(line)
        if match:
            start_str, end_str = match.groups()
            start = sum(float(x) * 60 ** i for i, x in enumerate(reversed(start_str.split(":"))))
            end = sum(float(x) * 60 ** i for i, x in enumerate(reversed(end_str.split(":"))))
            text_lines = []
            idx += 1
            while idx < len(lines) and lines[idx].strip() != "":
                text_lines.append(lines[idx].strip())
                idx += 1
            text = " ".join(text_lines)
            segments.append({"start": start, "end": end, "text": text})
        else:
            idx += 1
    return segments

def assign_speaker_to_segment(segment, diarization):
    seg_start = segment["start"]
    seg_end = segment["end"]
    max_overlap = 0.0
    speaker_assigned = "Desconhecido"
    
    for d_segment, _, speaker in diarization.itertracks(yield_label=True):
        overlap = max(0, min(seg_end, d_segment.end) - max(seg_start, d_segment.start))
        if overlap > max_overlap:
            max_overlap = overlap
            speaker_assigned = speaker
    return speaker_assigned

def merge_transcript_and_diarization(vtt_text, diarization):
    segments = parse_vtt(vtt_text)
    final_lines = []
    for seg in segments:
        speaker = assign_speaker_to_segment(seg, diarization)
        start_time = seg["start"]
        end_time = seg["end"]
        def format_time(s):
            hrs = int(s // 3600)
            mins = int((s % 3600) // 60)
            secs = s % 60
            return f"{hrs:02d}:{mins:02d}:{secs:05.2f}"
        time_str = f"[{format_time(start_time)} - {format_time(end_time)}]"
        final_lines.append(f"{time_str} {speaker}: {seg['text']}")
    return "\n".join(final_lines)

def classificar_falantes_com_gpt(texto_transcricao):
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""
        Analise esta transcrição de uma ligação de cobrança e identifique quem está falando em cada momento.
        
        Regras para identificar os falantes:
        - O Cliente geralmente inicia com "Alô", pergunta "quem é" ou "quem fala", e responde às perguntas
        - O Agente pergunta o nome do cliente. Ex:'Boa tarde, falo com a Giovana?' ou 'Falo com Raimundo?'
        - O Cliente pergunta o valor da dívida'
        - O Agente informa o valor da dívida'
        - O Agente geralmente se apresenta, dá bom dia, menciona a empresa, explica sobre débitos/cobranças
        - O Agente conduz a conversa fazendo perguntas sobre pagamentos
        - O Cliente geralmente responde às perguntas do agente
        - O Cliente pode alegar que já saiu do lugar de onde está sendo cobrado, por isso não reconhece a dívida
        - O Agente agenda o retorno, confirma o pagamento, e reforça a data-limite e as condições
        - O Agente forneçe informações sobre o pagamento, como valores, descontos e parcelas
        
        Formato da transcrição original:
        [TIMESTAMP] SPEAKER_ID: texto da fala
        
        Reescreva a transcrição no seguinte formato, SEMPRE na mesma linha:
        Agente: frase da fala do agente
        Cliente: frase da fala do cliente
        (Ou seja, cada linha deve começar com 'Agente:' ou 'Cliente:' seguido da fala, sem linhas separadas para o nome do falante e a fala, e sem texto extra.)
        
        Transcrição:
        {texto_transcricao}
        """
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Você é um assistente especializado em identificar falantes em transcrições de ligações de cobrança."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=4096
        )
        return response.choices[0].message.content.strip()
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
                response_format="text",
                prompt="Transcreva esta chamada completa entre um agente da Portes Advogados e um cliente",
                temperature=0.0  # Mais consistente
            )
        if hasattr(transcription_response, 'text'):
            text = transcription_response.text
        elif isinstance(transcription_response, str):
            text = transcription_response
        else:
            print(f"Formato de resposta desconhecido: {type(transcription_response)}")
            print(f"Conteúdo: {transcription_response}")
            return None
        
        # Verificar se a transcrição parece muito curta em relação ao áudio
        duracao = calcular_duracao_audio_robusto(caminho_audio)
        if duracao > 60:  # Só verifica áudios com mais de 1 minuto
            n_palavras = len(text.split())
            taxa_palavras = n_palavras / duracao
            if taxa_palavras < 1.5:  # Menos de 1.5 palavras por segundo é suspeito
                print(f"ALERTA: Transcrição potencialmente incompleta: {n_palavras} palavras em {duracao:.1f}s ({taxa_palavras:.2f} palavras/seg)")
        
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
        tempo_inicio = time.time()  # Marca o início do processamento do áudio
        final_text = process_audio_file(caminho_audio)
        if final_text:
            # Corrigir variações de "Portes Advogados" antes da classificação dos falantes
            final_text_corrigido = corrigir_portes_advogados(final_text)
            try:
                final_text_identificado = classificar_falantes_com_gpt(final_text_corrigido)
            except Exception as e:
                print(f"Erro ao identificar falantes com gpt-4.1-nano: {e}")
                final_text_identificado = final_text_corrigido
            nome_txt = os.path.splitext(arquivo)[0] + '_diarizado.txt'
            caminho_txt = os.path.join(pasta_transcricoes, nome_txt)
            with open(caminho_txt, 'w', encoding='utf-8') as f:
                f.write(final_text_identificado)
            print(f"Transcrição salva em: {caminho_txt}")
            caminho_destino = os.path.join(pasta_audios_transcritos, arquivo)
            try:
                import shutil
                shutil.move(caminho_audio, caminho_destino)
                print(f"Arquivo de áudio movido para: {caminho_destino}")
            except Exception as e:
                print(f"Erro ao mover o arquivo de áudio: {e}")
            # Salva o tempo de início do processamento para uso posterior
            with open(caminho_txt + '.start', 'w') as f:
                f.write(str(tempo_inicio))
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
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def process_transcription_folder(pasta_transcricoes):
    if not os.path.exists(pasta_transcricoes):
        print(f"Pasta de transcrições não encontrada: {pasta_transcricoes}")
        return
    
    pasta_transcricoes_avaliadas = os.path.join(pasta_transcricoes, 'Transcrições_avaliadas')
    pasta_transcricoes_erros = os.path.join(pasta_transcricoes, 'Transcrições_erros')
    
    os.makedirs(pasta_transcricoes_avaliadas, exist_ok=True)
    os.makedirs(pasta_transcricoes_erros, exist_ok=True)
    
    print(f"Pasta para transcrições avaliadas: {pasta_transcricoes_avaliadas}")
    print(f"Pasta para transcrições com erro: {pasta_transcricoes_erros}")
    
    arquivos_txt = [f for f in os.listdir(pasta_transcricoes) if f.endswith('.txt') and os.path.isfile(os.path.join(pasta_transcricoes, f))]
    
    if not arquivos_txt:
        print("Nenhuma transcrição encontrada para avaliação.")
        return
    
    print(f"Encontradas {len(arquivos_txt)} transcrições para avaliar.")
    
    for arquivo in arquivos_txt:
        caminho_transcricao = os.path.join(pasta_transcricoes, arquivo)
        id_chamada = os.path.splitext(arquivo)[0]
        
        print(f"Avaliando transcrição: {arquivo}")
        
        try:
            with open(caminho_transcricao, 'r', encoding='utf-8') as f:
                conteudo_transcricao = f.read()
            
            avaliacao = avaliar_ligacao(conteudo_transcricao, id_chamada=id_chamada)
            if 'itens' in avaliacao:
                resultado_redistribuido = redistribuir_pesos_e_pontuacao(avaliacao['itens'])
                avaliacao['itens'] = resultado_redistribuido['itens']
                avaliacao['pontuacao_total'] = resultado_redistribuido['pontuacao_total']
                avaliacao['pontuacao_percentual'] = resultado_redistribuido['pontuacao_percentual']
            
            nome_avaliacao = f"{id_chamada}_avaliacao.json"
            caminho_avaliacao = os.path.join(pasta_transcricoes_avaliadas, nome_avaliacao)
            
            with open(caminho_avaliacao, 'w', encoding='utf-8') as f:
                json.dump(avaliacao, f, ensure_ascii=False, indent=2)
            
            print(f"Avaliação salva em: {caminho_avaliacao}")
            
            caminho_destino = os.path.join(pasta_transcricoes_avaliadas, arquivo)
            import shutil
            shutil.copy2(caminho_transcricao, caminho_destino)
            os.remove(caminho_transcricao)
            print(f"Transcrição movida para: {caminho_destino}")
            
            # Salva o tempo de término do processamento para uso posterior
            with open(caminho_avaliacao + '.end', 'w') as f:
                f.write(str(time.time()))
            
            nota = avaliacao.get('pontuacao_percentual', 0)
            status = "APROVADA" if nota >= 70 else "REPROVADA"
            
            nome_resumo = f"{id_chamada}_resumo.txt"
            caminho_resumo = os.path.join(pasta_transcricoes_avaliadas, nome_resumo)
            
            with open(caminho_resumo, 'w', encoding='utf-8') as f:
                f.write(f"Avaliação da ligação: {id_chamada}\n")
                f.write(f"Status: {status}\n")
                f.write(f"Pontuação: {nota:.2f}%\n\n")
                f.write("Itens avaliados:\n")
                
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
            
            try:
                import shutil
                caminho_destino_erro = os.path.join(pasta_transcricoes_erros, arquivo)
                shutil.copy2(caminho_transcricao, caminho_destino_erro)
                os.remove(caminho_transcricao)
                print(f"Transcrição movida para pasta de erros: {caminho_destino_erro}")
                
                log_path = os.path.join(pasta_transcricoes_erros, f"{id_chamada}_erro.txt")
                with open(log_path, 'w', encoding='utf-8') as log_file:
                    log_file.write(f"Erro ao avaliar a transcrição {arquivo}\n")
                    log_file.write(f"Data/hora: {format_time_now()}\n")
                    log_file.write(f"Erro: {str(e)}")
                print(f"Log de erro criado em: {log_path}")
            except Exception as move_error:
                print(f"Erro ao mover a transcrição com falha: {move_error}")

def avaliar_ligacao(transcricao: str, *, 
                    id_chamada: str = "chamada‑sem‑id") -> Dict[str, Any]:
    client = _get_client()

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",
         "content": f"ID_CHAMADA={id_chamada}\n\nTRANSCRICAO:\n{transcricao}"}
    ]

    print(f"Avaliando ligação: {id_chamada}")
    response = client.chat.completions.create(
        model="gpt-4.1-mini",  
        messages=messages,
        temperature=0.0,
        max_tokens=1024
    )

    assistant_content = response.choices[0].message.content.strip()

    try:
        result = json.loads(assistant_content)
        total = result.get("pontuacao_total", 0)
        result["pontuacao_percentual"] = round((total / MAX_SEM_GSS) * 100, 1)
        print(f"Avaliação concluída para ligação: {id_chamada}")
        return result
    except json.JSONDecodeError as e:
        error_msg = f"Resposta não é JSON válido para ligação {id_chamada}"
        print(f"ERRO: {error_msg}")
        raise RuntimeError(error_msg) from e

def redistribuir_pesos_e_pontuacao(itens: dict) -> dict:
    """
    Redistribui os pesos das categorias ignorando as que receberam 'N/A' e o item 'Falha Critica'.
    Se 'Falha Critica' for 'Não Conforme', a nota é 0%. Caso contrário, a nota é calculada normalmente.
    """
    subitens = []
    falha_critica_nao_conforme = False
    for categoria, subdict in itens.items():
        for nome, info in subdict.items():
            # Detecta Falha Critica Não Conforme
            if categoria.strip().lower() == 'falha critica' and info.get('status', '').strip().upper() == 'NÃO CONFORME':
                falha_critica_nao_conforme = True
            subitens.append((categoria, nome, info))
    # Filtrar subitens válidos (não N/A e não Falha Critica)
    subitens_validos = [s for s in subitens if s[0].strip().lower() != 'falha critica' and s[2].get('status', '').strip().upper() not in ['N/A', 'NA', 'N\A', 'N. A.']]
    n_validos = len(subitens_validos)
    if n_validos == 0:
        return itens
    peso_redistribuido = 1.0 / n_validos
    # Atribuir novo peso para cada subitem válido e zerar para N/A e Falha Critica
    for categoria, nome, info in subitens:
        if categoria.strip().lower() != 'falha critica' and info.get('status', '').strip().upper() not in ['N/A', 'NA', 'N\A', 'N. A.']:
            info['peso'] = round(peso_redistribuido, 4)
        else:
            info['peso'] = 0.0
    # Calcular pontuação total redistribuída (só soma os 'Conforme', exceto Falha Critica)
    pontuacao_total = 0.0
    for categoria, nome, info in subitens:
        if categoria.strip().lower() != 'falha critica' and info.get('status', '').strip().upper() == 'CONFORME':
            pontuacao_total += info['peso']
    # Se Falha Critica for Não Conforme, zera a nota
    if falha_critica_nao_conforme:
        pontuacao_total = 0.0
    return {
        'itens': itens,
        'pontuacao_total': round(pontuacao_total * 10, 2),
        'pontuacao_percentual': round(pontuacao_total * 100, 1)
    }

def gerar_csv_relatorio_avaliacoes(pasta_avaliacoes, csv_saida):
    import csv
    import re
    categorias_relatorio = [
        'Abordagem',
        'Segurança',
        'Fraseologia de Momento e Retorno',
        'Comunicação',
        'Cordialidade',
        'Empatia',
        'Escuta Ativa',
        'Clareza & Objetividade',
        'Oferta de Solução & Condições',
        'Confirmação de Aceite',
        'Reforço de Prazo & Condições',
        'Encerramento',
#   'Registro no GSS',
        'Falha Critica'
    ]
    campos = ['data', 'agente', 'fila'] + categorias_relatorio + ['pontuacao_percentual']
    import glob
    arquivos_json = glob.glob(os.path.join(pasta_avaliacoes, '*_avaliacao.json'))
    if not arquivos_json:
        print(f"Nenhum arquivo de avaliação encontrado em {pasta_avaliacoes}")
        return
    linhas = []
    for caminho_json in arquivos_json:
        with open(caminho_json, encoding='utf-8') as f:
            dados = json.load(f)
        nome_arquivo = os.path.basename(caminho_json)
        m = re.match(r'(\d{8})_\d{6}_Agente_(\d+)_Fila_(.+?)_diarizado_?avaliacao.json', nome_arquivo)
        if not m:
            m = re.match(r'(\d{8})_\d{6}_Agente_(\d+)_Fila_(.+?)_avaliacao.json', nome_arquivo)
        if m:
            data_str, agente, fila = m.groups()
            data_fmt = f"{data_str[:4]}-{data_str[4:6]}-{data_str[6:]}"
        else:
            data_fmt, agente, fila = '', '', ''
        linha = [data_fmt, agente, fila.replace('_', ' ')]
        for categoria in categorias_relatorio:
            status = ''
            itens_categoria = dados.get('itens', {}).get(categoria, {})
            if itens_categoria:
                primeiro = next(iter(itens_categoria.values()))
                if isinstance(primeiro, dict):
                    status = primeiro.get('status', '')
                elif isinstance(primeiro, str):
                    status = primeiro
            linha.append(status)
        linha.append(dados.get('pontuacao_percentual', ''))
        linhas.append(linha)
    with open(csv_saida, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(campos)
        writer.writerows(linhas)
    print(f"CSV consolidado gerado em: {csv_saida}")

def calcular_duracao_audio_robusto(caminho_audio):
    try:
        info = mediainfo(caminho_audio)
        duracao = float(info['duration'])
        return duracao  # em segundos
    except Exception as e:
        print(f"Erro ao calcular duração de {caminho_audio}: {e}")
        return 0

def verificar_transcricoes_incompletas(pasta_audios, pasta_transcricoes):
    """
    Percorre todos os áudios e transcrições, compara duração do áudio e tamanho da transcrição.
    Alerta se a transcrição parecer muito curta em relação ao áudio.
    """
    pasta_audios_transcritos = os.path.join(pasta_audios, 'Audios_transcritos')
    pasta_transcricoes = os.path.join(pasta_transcricoes)
    
    print("\n=== VERIFICANDO POSSÍVEIS TRANSCRIÇÕES INCOMPLETAS ===")
    
    # Encontrar todos os arquivos de áudio
    extensoes_audio = ['.mp3', '.wav', '.m4a', '.ogg', '.flac']
    arquivos_audio = []
    for ext in extensoes_audio:
        arquivos_audio += glob.glob(os.path.join(pasta_audios_transcritos, f'*{ext}'))
    
    if not arquivos_audio:
        print("Nenhum áudio transcrito encontrado para verificação.")
        return
    
    problemas_encontrados = 0
    relatorio = []
    
    for caminho_audio in arquivos_audio:
        nome_base = os.path.splitext(os.path.basename(caminho_audio))[0]
        caminho_txt = os.path.join(pasta_transcricoes, nome_base + '_diarizado.txt')
        
        if not os.path.exists(caminho_txt):
            msg = f"ERRO: Transcrição não encontrada para: {nome_base}"
            print(msg)
            relatorio.append({
                'arquivo': nome_base,
                'problema': 'Transcrição não encontrada',
                'duracao_audio': 'N/A',
                'palavras': 0,
                'taxa_palavras_seg': 0
            })
            problemas_encontrados += 1
            continue
        
        duracao = calcular_duracao_audio_robusto(caminho_audio)
        
        with open(caminho_txt, 'r', encoding='utf-8') as f:
            texto = f.read()
        
        # Calcula estatísticas
        n_palavras = len(texto.split())
        n_caracteres = len(texto)
        taxa_palavras = n_palavras / duracao if duracao > 0 else 0
        
        # Heurísticas para detectar possíveis transcrições incompletas:
        # 1. Menos de 1.5 palavras por segundo (ligação normal tem ~2-3 palavras/seg)
        # 2. Menos de 9 caracteres por segundo
        # 3. Áudio longo (>60s) com menos de 100 palavras
        
        problema = None
        if duracao > 60 and n_palavras < 100:
            problema = f"Áudio de {duracao:.1f}s tem apenas {n_palavras} palavras"
        elif duracao > 0 and taxa_palavras < 1.5:
            problema = f"Taxa baixa: {taxa_palavras:.2f} palavras/seg"
        elif duracao > 0 and (n_caracteres / duracao) < 9:
            problema = f"Poucos caracteres por segundo: {(n_caracteres/duracao):.2f}"
            
        if problema:
            print(f"ALERTA: Possível transcrição incompleta: {nome_base}")
            print(f"  Duração: {duracao:.1f}s, Palavras: {n_palavras}, Taxa: {taxa_palavras:.2f} palavras/seg")
            print(f"  Problema: {problema}")
            relatorio.append({
                'arquivo': nome_base,
                'problema': problema,
                'duracao_audio': f"{duracao:.1f}",
                'palavras': n_palavras,
                'taxa_palavras_seg': f"{taxa_palavras:.2f}"
            })
            problemas_encontrados += 1
    
    # Gera relatório CSV se houver problemas
    if problemas_encontrados > 0:
        relatorio_csv = os.path.join(pasta_audios, 'relatorio_transcricoes_incompletas.csv')
        with open(relatorio_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['arquivo', 'problema', 'duracao_audio', 'palavras', 'taxa_palavras_seg'])
            writer.writeheader()
            for linha in relatorio:
                writer.writerow(linha)
        print(f"\nEncontrados {problemas_encontrados} possíveis problemas de transcrição.")
        print(f"Relatório detalhado salvo em: {relatorio_csv}")
    else:
        print("\nNenhum problema de transcrição detectado!")

    return problemas_encontrados

def gerar_relatorio_gastos(pasta_audios, pasta_transcricoes, relatorio_saida,
                          modelo_transcricao='gpt-4o-transcribe',
                          preco_min_transcricao=0.006,
                          modelo_avaliacao='gpt-4.1-nano',
                          preco_avaliacao_por_chamada=0.002):
    import csv
    import glob
    import re
    from datetime import datetime
    
    arquivos_audio = glob.glob(os.path.join(pasta_audios, 'Audios_transcritos', '*.wav'))
    arquivos_audio += glob.glob(os.path.join(pasta_audios, 'Audios_transcritos', '*.mp3'))
    arquivos_audio += glob.glob(os.path.join(pasta_audios, 'Audios_transcritos', '*.m4a'))
    arquivos_audio += glob.glob(os.path.join(pasta_audios, 'Audios_transcritos', '*.flac'))
    arquivos_audio += glob.glob(os.path.join(pasta_audios, 'Audios_transcritos', '*.ogg'))
    
    pasta_avaliacoes = os.path.join(pasta_transcricoes, 'Transcrições_avaliadas')
    arquivos_json = glob.glob(os.path.join(pasta_avaliacoes, '*_avaliacao.json'))
    
    avaliacoes = {}
    for caminho_json in arquivos_json:
        nome_base = os.path.basename(caminho_json).split('_avaliacao.json')[0]
        avaliacoes[nome_base] = caminho_json
    
    linhas = []
    total_transcricao = 0
    total_avaliacao = 0
    total_geral = 0
    for caminho_audio in arquivos_audio:
        if not os.path.exists(caminho_audio):
            print(f"Arquivo não encontrado, pulando: {caminho_audio}")
            continue
        nome_audio = os.path.basename(caminho_audio)
        nome_base = os.path.splitext(nome_audio)[0]
        m = re.match(r'(\d{8})_\d{6}_Agente_(\d+)_Fila_(.+)', nome_base)
        if m:
            data_str, agente, fila = m.groups()
            # Remover extensão da fila, se houver
            fila = os.path.splitext(fila)[0]
            data_fmt = f"{data_str[:4]}-{data_str[4:6]}-{data_str[6:]}"
        else:
            data_fmt, agente, fila = '', '', ''
        duracao_seg = calcular_duracao_audio_robusto(caminho_audio)
        duracao_min = duracao_seg / 60
        custo_transcricao = duracao_min * preco_min_transcricao
        nome_base_aval = nome_base + '_diarizado' if (nome_base + '_diarizado') in avaliacoes else nome_base
        custo_avaliacao = preco_avaliacao_por_chamada if nome_base_aval in avaliacoes else 0
        custo_total = custo_transcricao + custo_avaliacao
        total_transcricao += custo_transcricao
        total_avaliacao += custo_avaliacao
        total_geral += custo_total
        # Busca tempo de processamento
        tempo_processamento = ''
        caminho_txt = os.path.join(pasta_transcricoes, nome_base + '_diarizado.txt')
        caminho_start = caminho_txt + '.start'
        caminho_avaliacao = os.path.join(pasta_transcricoes, 'Transcrições_avaliadas', nome_base + '_diarizado_avaliacao.json')
        caminho_end = caminho_avaliacao + '.end'
        try:
            if os.path.exists(caminho_start) and os.path.exists(caminho_end):
                with open(caminho_start) as f:
                    t_start = float(f.read().strip())
                with open(caminho_end) as f:
                    t_end = float(f.read().strip())
                tempo_processamento = t_end - t_start
        except Exception as e:
            tempo_processamento = ''
        linhas.append([
            nome_audio, data_fmt, agente, fila.replace('_', ' '),
            f"{duracao_min:.2f}",
            f"${custo_transcricao:.4f}",
            f"${custo_avaliacao:.4f}",
            f"${custo_total:.4f}",
            f"{tempo_processamento:.2f}" if tempo_processamento != '' else ''
        ])
    campos = [
        'arquivo', 'data', 'agente', 'fila', 'duração_min',
        f'custo_transcricao_{modelo_transcricao}_usd',
        f'custo_avaliacao_{modelo_avaliacao}_usd',
        'custo_total_usd',
        'tempo_processamento_segundos'
    ]
    with open(relatorio_saida, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(campos)
        writer.writerows(linhas)
        writer.writerow(['TOTAL', '', '', '', '',
                         f"${total_transcricao:.4f}",
                         f"${total_avaliacao:.4f}",
                         f"${total_geral:.4f}"])
    print(f"Relatório de gastos gerado em: {relatorio_saida}")

if __name__ == '__main__':
    pasta_audios = r'C:\Users\wanderley.terra\Documents\Audios_monitoria'
    
    process_audio_folder(pasta_audios)
    
    pasta_transcricoes = os.path.join(pasta_audios, 'Transcrições_aguas')
    process_transcription_folder(pasta_transcricoes)
    
    pasta_avaliacoes = os.path.join(pasta_transcricoes, 'Transcrições_avaliadas')
    csv_saida = os.path.join(pasta_avaliacoes, 'relatorio_avaliacoes.csv')
    gerar_csv_relatorio_avaliacoes(pasta_avaliacoes, csv_saida)
    
    relatorio_gastos_saida = os.path.join(pasta_audios, 'relatorio_gastos.csv')
    gerar_relatorio_gastos(
        pasta_audios=pasta_audios,
        pasta_transcricoes=pasta_transcricoes,
        relatorio_saida=relatorio_gastos_saida,
        modelo_transcricao='gpt-4o-transcribe',
        preco_min_transcricao=0.006,
        modelo_avaliacao='gpt-4.1-nano',
        preco_avaliacao_por_chamada=0.002
    )
    
    # Executar verificação de transcrições incompletas ao final do processo
    verificar_transcricoes_incompletas(pasta_audios, pasta_transcricoes)
    
    print("Processamento completo!")
