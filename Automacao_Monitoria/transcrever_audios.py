import os
import sys
import re
import json
import warnings
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
import openai
import re
from openai import OpenAI


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
✔ Confirmar nome/CPF e endereço antes da negociação
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
                "status": "Conforme|Não Conforme|N/A",
                "peso": 0.25,
                "observacao": "texto livre curto"
            }}
            }},
            ...
            "Falha Critica": {{
            "Sem falha crítica": {{
                "status": "Conforme|Não Conforme",
                "peso": 0
            }}
            }}
        }},
        "pontuacao_total": 0‑10,
        "pontuacao_percentual": 0‑100
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
            model="gpt-4.1-nano",
            messages=[
                {"role": "system", "content": "Você é um assistente especializado em identificar falantes em transcrições de ligações de cobrança."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=4096
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Erro ao classificar falantes com GPT-4.1-nano: {e}")
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
        model="gpt-4.1-nano",  
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
    subitens = []
    for categoria, subdict in itens.items():
        for nome, info in subdict.items():
            subitens.append((categoria, nome, info))
    subitens_validos = [s for s in subitens if s[2].get('status', '').strip().upper() not in ['N/A', 'NA', 'N\A', 'N. A.']]
    n_validos = len(subitens_validos)
    if n_validos == 0:
        return itens
    peso_redistribuido = 1.0 / n_validos
    for categoria, nome, info in subitens:
        if info.get('status', '').strip().upper() not in ['N/A', 'NA', 'N\A', 'N. A.']:
            info['peso'] = round(peso_redistribuido, 4)
        else:
            info['peso'] = 0.0
    pontuacao_total = 0.0
    for categoria, nome, info in subitens:
        if info.get('status', '').strip().upper() == 'CONFORME':
            pontuacao_total += info['peso']
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

import wave

def calcular_duracao_audio_wav(caminho_audio):
    try:
        with wave.open(caminho_audio, 'rb') as wf:
            frames = wf.getnframes()
            rate = wf.getframerate()
            duracao = frames / float(rate)
            return duracao
    except Exception as e:
        print(f"Erro ao calcular duração de {caminho_audio}: {e}")
        return 0

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
        nome_audio = os.path.basename(caminho_audio)
        nome_base = os.path.splitext(nome_audio)[0]
        m = re.match(r'(\d{8})_\d{6}_Agente_(\d+)_Fila_(.+)', nome_base)
        if m:
            data_str, agente, fila = m.groups()
            data_fmt = f"{data_str[:4]}-{data_str[4:6]}-{data_str[6:]}"
        else:
            data_fmt, agente, fila = '', '', ''
        duracao_seg = calcular_duracao_audio_wav(caminho_audio)
        duracao_min = duracao_seg / 60
        custo_transcricao = duracao_min * preco_min_transcricao
        nome_base_aval = nome_base + '_diarizado' if (nome_base + '_diarizado') in avaliacoes else nome_base
        custo_avaliacao = preco_avaliacao_por_chamada if nome_base_aval in avaliacoes else 0
        custo_total = custo_transcricao + custo_avaliacao
        total_transcricao += custo_transcricao
        total_avaliacao += custo_avaliacao
        total_geral += custo_total
        linhas.append([
            nome_audio, data_fmt, agente, fila.replace('_', ' '),
            f"{duracao_min:.2f}",
            f"${custo_transcricao:.4f}",
            f"${custo_avaliacao:.4f}",
            f"${custo_total:.4f}"
        ])
    campos = [
        'arquivo', 'data', 'agente', 'fila', 'duração_min',
        f'custo_transcricao_{modelo_transcricao}_usd',
        f'custo_avaliacao_{modelo_avaliacao}_usd',
        'custo_total_usd'
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
    
    print("Processamento completo!")
