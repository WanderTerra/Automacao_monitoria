import json
import os
import re
import sys
import time
import csv
import glob
import shutil
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv
import openai
from openai import OpenAI
from pydub.utils import mediainfo
import mysql.connector
from mysql.connector import Error as MySQLError

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

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except MySQLError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        raise

def carregar_mapeamento_call_ids(pasta_audios: str) -> dict:
    """
    Carrega o mapeamento entre nomes de arquivos e call_ids originais.
    """
    arquivo_mapeamento = os.path.join(pasta_audios, 'mapeamento_call_ids.csv')
    mapeamento = {}
    try:
        with open(arquivo_mapeamento, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapeamento[row['nome_arquivo']] = row['call_id']
        print(f"Mapeamento de call_ids carregado com {len(mapeamento)} registros.")
    except FileNotFoundError:
        print("AVISO: Arquivo de mapeamento não encontrado. Usando método de busca por data/hora.")
    return mapeamento

# Variável global para armazenar o mapeamento
mapeamento_call_ids = {}

def extrair_call_id_original(nome_arquivo: str) -> str:
    """
    Obtém o call_id original usando o mapeamento ou, se não disponível,
    busca no banco de dados com base na data e hora.
    """
    # Primeiro tenta usar o mapeamento
    if nome_arquivo in mapeamento_call_ids:
        return mapeamento_call_ids[nome_arquivo]
    
    # Se não encontrou no mapeamento, usa o método antigo
    try:
        match = re.match(r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})_Agente_(\d+)', nome_arquivo)
        if not match:
            return None
        
        year, month, day, hour, minute, second, agent_id = match.groups()
        data_hora = f"{year}-{month}-{day} {hour}:{minute}:{second}"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Busca o call_id próximo ao horário do arquivo (com margem de 5 minutos)
        query = """
        SELECT call_id FROM vonix.calls
        WHERE agent_id = %s
        AND ABS(TIMESTAMPDIFF(SECOND, start_time, %s)) < 300
        ORDER BY ABS(TIMESTAMPDIFF(SECOND, start_time, %s))
        LIMIT 1
        """
        
        cursor.execute(query, (agent_id, data_hora, data_hora))
        resultado = cursor.fetchone()
        
        if resultado:
            return resultado[0]
        return None
        
    except Exception as e:
        print(f"Erro ao buscar call_id original: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def map_resultado_value(status: str) -> str:
    """Maps the status values to database-compatible resultado values"""
    status_map = {
        'C': 'CONFORME',
        'NC': 'NAO CONFORME',
        'NA': 'NAO SE APLICA',
        'N/A': 'NAO SE APLICA',
        'N\\A': 'NAO SE APLICA',
        'N. A.': 'NAO SE APLICA'
    }
    return status_map.get(status.upper(), 'NAO SE APLICA')

def extrair_descricao_e_peso(categoria: str, arquivo_resumo: str) -> tuple:
    """
    Extrai a descrição completa e o peso de um item do arquivo de resumo
    Retorna uma tupla (descrição, peso)
    """
    try:
        with open(arquivo_resumo, 'r', encoding='utf-8') as f:
            conteudo = f.read()
        
        # Procura pelo bloco do item específico
        categoria_formatada = categoria.replace('_', ' ').title().strip()
        blocos = conteudo.split('\n\n')
        for bloco in blocos:
            if categoria_formatada in bloco:
                # Extrai o peso (número entre parênteses)
                peso_match = re.search(r'\(([0-9.]+)\)', bloco)
                peso = float(peso_match.group(1)) if peso_match else 0.0
                
                # Extrai a descrição (texto após o hífen)
                desc_match = re.search(r'-\s*(.+?)(?=\n|$)', bloco)
                descricao = desc_match.group(1).strip() if desc_match else ''
                
                return descricao, peso
    except Exception as e:
        print(f"Erro ao extrair descrição e peso para {categoria}: {e}")
    
    return '', 0.0

def salvar_avaliacao_no_banco(avaliacao: dict, transcricao_texto: str = None):
    conn = None
    cursor = None
    try:        
        # Obter o nome base do arquivo
        id_chamada = avaliacao['id_chamada']
        nome_base = os.path.splitext(os.path.basename(id_chamada))[0]
        
        # Obter a data do arquivo usando a data associada ao call_id
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT start_time 
                FROM vonix.calls 
                WHERE call_id = %s
                """, (extrair_call_id_original(os.path.basename(id_chamada)),))
            resultado = cursor.fetchone()
            if resultado:
                data_ligacao = resultado[0].strftime('%Y-%m-%d %H:%M:%S')
            else:
                data_ligacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print("AVISO: Não foi possível encontrar a data da ligação no banco, usando data atual.")
        except Exception as e:
            print(f"Erro ao buscar data da ligação: {e}")
            data_ligacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        finally:
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()
        
        # Calcular a pontuação total baseado nos itens
        total_validos = 0
        total_conforme = 0
        for item in avaliacao.get('itens', {}).values():
            if isinstance(item, dict):
                status = item.get('status', 'NA')
            else:
                status = item if isinstance(item, str) else 'NA'
                
            resultado = map_resultado_value(status)
            if resultado != 'NAO SE APLICA':
                total_validos += 1
                if resultado == 'CONFORME':
                    total_conforme += 1
        
        pontuacao = (total_conforme / total_validos * 100) if total_validos > 0 else 0
        status_avaliacao = 'APROVADA' if pontuacao >= 70 else 'REPROVADA'
        
        # Debug dos dados que serão salvos
        print("\n============ DADOS PARA INSERÇÃO NO BANCO ============")
        print(f"TABELA avaliacoes:")
        print(f"- call_id: {extrair_call_id_original(os.path.basename(avaliacao['id_chamada']))}")
        print(f"- agent_id: {extrair_agent_id(avaliacao['id_chamada'])}")
        print(f"- data_ligacao: {data_ligacao}")
        print(f"- status_avaliacao: {status_avaliacao}")
        print(f"- pontuacao: {pontuacao}")
        print(f"- carteira: AGUAS")
        
        print("\nTABELA itens_avaliados:")
        for categoria, item in avaliacao.get('itens', {}).items():
            print(f"- categoria: {categoria}")
            if isinstance(item, dict):
                status = item.get('status', 'NA')
                observacao = item.get('observacao', '')
                peso = 1.0  # Peso base, será redistribuído depois
            else:
                status = item if isinstance(item, str) else 'NA'
                observacao = ''
                peso = 1.0
                
            resultado = map_resultado_value(status)
            print(f"  descricao: {observacao}")
            print(f"  resultado: {resultado}")
            print(f"  peso: {peso}")
            print("  ---")
                
        print("====================================================\n")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Obter o call_id original
        call_id_original = extrair_call_id_original(os.path.basename(id_chamada))
        
        if not call_id_original:
            raise ValueError(f"Não foi possível encontrar o call_id original para {id_chamada}")
        
        agent_id = extrair_agent_id(id_chamada)
        carteira = 'AGUAS'  # Valor fixo para este contexto

        # Inserir na tabela avaliacoes
        sql_avaliacao = """
        INSERT INTO avaliacoes (call_id, agent_id, data_ligacao, status_avaliacao, pontuacao, carteira)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_avaliacao, (call_id_original, agent_id, data_ligacao, status_avaliacao, pontuacao, carteira))
        id_avaliacao = cursor.lastrowid

        # Calcular o total de itens não-NA para redistribuir os pesos
        itens_validos = sum(1 for item in avaliacao.get('itens', {}).values() 
                          if isinstance(item, dict) and map_resultado_value(item.get('status', 'NA')) != 'NAO SE APLICA')
        peso_por_item = round(1.0 / itens_validos if itens_validos > 0 else 0.0, 4)

        # Inserir os itens avaliados
        sql_itens = """
        INSERT INTO itens_avaliados (avaliacao_id, categoria, descricao, resultado, peso)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        for categoria, item in avaliacao.get('itens', {}).items():
            if isinstance(item, dict):
                resultado = map_resultado_value(item.get('status', 'NA'))
                observacao = item.get('observacao', '')
            else:
                resultado = map_resultado_value(item if isinstance(item, str) else 'NA')
                observacao = ''
                
            peso = peso_por_item if resultado != 'NAO SE APLICA' else 0.0
            cursor.execute(sql_itens, (id_avaliacao, categoria, observacao, resultado, peso))

        # Buscar e inserir o conteúdo da transcrição
        # Usa apenas a transcrição passada pela variável, não lê mais o arquivo txt
        conteudo_transcricao = transcricao_texto
        if conteudo_transcricao:
            # Verifica se já existe transcrição para este avaliacao_id
            sql_check = "SELECT id FROM transcricoes WHERE avaliacao_id = %s"
            cursor.execute(sql_check, (id_avaliacao,))
            existe = cursor.fetchone()
            if existe:
                sql_update = "UPDATE transcricoes SET conteudo = %s WHERE avaliacao_id = %s"
                cursor.execute(sql_update, (conteudo_transcricao, id_avaliacao))
                print(f"Transcrição atualizada para avaliacao_id: {id_avaliacao}")
            else:
                sql_transcricao = """
                INSERT INTO transcricoes (avaliacao_id, conteudo)
                VALUES (%s, %s)
                """
                cursor.execute(sql_transcricao, (id_avaliacao, conteudo_transcricao))
                print(f"Transcrição inserida com sucesso para avaliacao_id: {id_avaliacao}")
        else:
            print(f"AVISO: Conteúdo da transcrição vazio, não foi possível inserir no banco. Valor recebido: {repr(transcricao_texto)}")

        conn.commit()
        print(f"Avaliação do call_id {call_id_original} salva no banco com sucesso!")
        
    except Exception as e:
        print(f"Erro ao salvar avaliação no banco: {e}")
        if conn and conn.is_connected():
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def extrair_agent_id(id_chamada: str) -> str:
    """Extrai o ID do agente do nome do arquivo de chamada."""
    import re
    match = re.search(r'Agente_(\d+)', id_chamada)
    return match.group(1) if match else None

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
Você é **MonitorGPT**, auditor de qualidade das ligações da carteira **Águas Guariroba** (Portes Advogados).

---
ENUM `status`
- **C**  → Conforme  
- **NC** → Não Conforme  
- **NA** → Não Se Aplica

FALHA CRÍTICA  
Se detectar ofensa, vazamento de dado sensível ou transferência sem aviso: defina `falha_critica = true`.  
Em caso contrário, `falha_critica = false`.

---
CHECKLIST DE AVALIAÇÃO (12 sub‑itens)
1. **Abordagem**              `abordagem_atendeu` – Atendeu prontamente?
2. **Segurança**              `seguranca_info_corretas` – Atendimento seguro, sem informações falsas?
3. **Fraseologia**            `fraseologia_explica_motivo` – Explicou motivo de ausência/transferência?
4. **Comunicação**            `comunicacao_tom_adequado` – Tom de voz adequado, linguagem clara, sem gírias?
5. **Cordialidade**           `cordialidade_respeito` – Respeitoso, sem comentários impróprios?
6. **Empatia**                `empatia_genuina` – Demonstrou empatia genuína?
7. **Escuta Ativa**           `escuta_sem_interromper` – Ouviu sem interromper, retomando pontos?
8. **Clareza & Objetividade** `clareza_direta` – Explicações diretas, sem rodeios?
9. **Oferta de Solução**      `oferta_valores_corretos` – Apresentou valores, descontos e opções corretamente? *(aplica‑se só se cliente permitir)*
10. **Confirmação de Aceite** `confirmacao_aceite` – Confirmou negociação com “sim, aceito/confirmo”? *(aplica‑se só se houve negociação)*
11. **Reforço de Prazo**      `reforco_prazo` – Reforçou data‑limite e perda de desconto? *(aplica‑se só se fechou acordo)*
12. **Encerramento**          `encerramento_agradece` – Perguntou “Posso ajudar em algo mais?” e agradeceu? *(aplica‑se só se fechou acordo)*

REGRAS DE CONFORMIDADE EXTRA (verificar além do checklist)
- Identificar‑se: NOME + “Portes Advogados assessoria jurídica das Águas Guariroba”.
- Confirmar nome **ou** CPF **e** endereço antes da negociação.
- Ofertar valor total, valor com desconto, entrada e parcelas ≥ R$ 20,00.
- Perguntar se o número tem WhatsApp antes de enviar boleto.
- Reforçar: “Pagamento até X às 18h ou perderá o desconto”.

---
## SCHEMA DE SAÍDA (JSON)
```json
{{
  "id_chamada": "string",
  "avaliador": "MonitorGPT",
  "falha_critica": false,
  "itens": {{
    "abordagem_atendeu":        {{"status": "C|NC|NA", "observacao": ""}},
    "seguranca_info_corretas":  {{"status": "C|NC|NA", "observacao": ""}},
    "fraseologia_explica_motivo": {{"status": "C|NC|NA", "observacao": ""}},
    "comunicacao_tom_adequado": {{"status": "C|NC|NA", "observacao": ""}},
    "cordialidade_respeito":    {{"status": "C|NC|NA", "observacao": ""}},
    "empatia_genuina":          {{"status": "C|NC|NA", "observacao": ""}},
    "escuta_sem_interromper":   {{"status": "C|NC|NA", "observacao": ""}},
    "clareza_direta":           {{"status": "C|NC|NA", "observacao": ""}},
    "oferta_valores_corretos":  {{"status": "C|NC|NA", "observacao": ""}},
    "confirmacao_aceite":       {{"status": "C|NC|NA", "observacao": ""}},
    "reforco_prazo":            {{"status": "C|NC|NA", "observacao": ""}},
    "encerramento_agradece":    {{"status": "C|NC|NA", "observacao": ""}}
  }}
}}
```

⚠️ **Instruções finais**
1. Avalie cada sub‑item: escolha status `C`, `NC` ou `NA`.
2. Preencha `observacao` com até 15 palavras (ou deixe string vazia).  
3. Preencha `falha_critica` conforme definido.
4. **Não** inclua campos de peso nem pontuações.
5. **Responda SOMENTE** o JSON acima – sem Markdown, sem texto extra.
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
        r'porto advogados',
        r'porta de jogados',
        r'parque dos advogados'
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
    global mapeamento_call_ids
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
    
    # Carrega o mapeamento de call_ids
    mapeamento_call_ids = carregar_mapeamento_call_ids(pasta)
    
    for arquivo in arquivos:
        caminho_audio = os.path.join(pasta, arquivo)
        print(f"Processando: {arquivo}")
        tempo_inicio = time.time()  # Marca o início do processamento do áudio
        final_text = process_audio_file(caminho_audio)
        nome_base = os.path.splitext(arquivo)[0]
        caminho_destino = os.path.join(pasta_audios_transcritos, arquivo)
        try:
            if final_text:
                # Corrigir variações de "Portes Advogados" antes da classificação dos falantes
                final_text_corrigido = corrigir_portes_advogados(final_text)
                try:
                    final_text_identificado = classificar_falantes_com_gpt(final_text_corrigido)
                except Exception as e:
                    print(f"Erro ao identificar falantes com gpt-4.1-nano: {e}")
                    final_text_identificado = final_text_corrigido

                try:
                    avaliacao_simples = {
                        "id_chamada": nome_base,
                        "avaliador": "MonitorGPT",
                        "falha_critica": False,
                        "itens": {}
                    }

                except Exception as e:
                    print(f"Erro ao preparar avaliação simples: {e}")

                # Depois salva no arquivo
                nome_txt = nome_base + '_diarizado.txt'
                caminho_txt = os.path.join(pasta_transcricoes, nome_txt)
                try:
                    with open(caminho_txt, 'w', encoding='utf-8') as f:
                        f.write(final_text_identificado)
                    print(f"Transcrição salva em arquivo: {caminho_txt}")
                    # Salva o tempo de início do processamento para uso posterior
                    with open(caminho_txt + '.start', 'w') as f:
                        f.write(str(tempo_inicio))
                except Exception as e:
                    print(f"Erro ao salvar transcrição em arquivo: {e}")
            else:
                caminho_erro = os.path.join(pasta_erros, arquivo)
                try:
                    shutil.move(caminho_audio, caminho_erro)
                    print(f"Falha ao processar {arquivo} - Arquivo movido para pasta de erros: {caminho_erro}")
                    log_path = os.path.join(pasta_erros, f"{os.path.splitext(arquivo)[0]}_erro.txt")
                    with open(log_path, 'w', encoding='utf-8') as log_file:
                        log_file.write(f"Erro ao processar o arquivo {arquivo}\n")
                        log_file.write(f"Data/hora: {format_time_now()}\n")
                        log_file.write(f"Falha na transcrição ou identificação de falantes")
                except Exception as move_error:
                    print(f"Erro ao mover o arquivo com falha: {move_error}")
                return  # Não tenta mover para transcritos se falhou
        finally:
            # Garante que o áudio seja movido para a pasta de transcritos se não foi movido para erros
            if os.path.exists(caminho_audio):
                try:
                    shutil.move(caminho_audio, caminho_destino)
                    print(f"Arquivo de áudio movido para: {caminho_destino}")
                except Exception as e:
                    print(f"Erro ao mover o arquivo de áudio: {e}")


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
            
            # Garantir que a avaliação retorne um dicionário
            avaliacao = avaliar_ligacao(conteudo_transcricao, id_chamada=id_chamada)
            if isinstance(avaliacao, str):
                try:
                    avaliacao = json.loads(avaliacao)
                except json.JSONDecodeError:
                    # Se não conseguir converter para JSON, cria um dicionário padrão
                    avaliacao = {
                        "id_chamada": id_chamada,
                        "avaliador": "MonitorGPT",
                        "falha_critica": True,
                        "itens": {},
                        "erro_processamento": "Falha ao decodificar JSON da avaliação",
                        "pontuacao_total": 0,
                        "pontuacao_percentual": 0
                    }
            
            # Adiciona campos necessários se faltando
            avaliacao['id_chamada'] = avaliacao.get('id_chamada', id_chamada)
            avaliacao['itens'] = avaliacao.get('itens', {})
            avaliacao['pontuacao_percentual'] = avaliacao.get('pontuacao_percentual', 0)
            
            # Não salva transcrição no banco neste fluxo!
            # Apenas avalia e registra os itens avaliados, sem inserir transcrição
            try:
                salvar_avaliacao_no_banco(avaliacao, transcricao_texto=None)
                print(f"[DEBUG] Inserção no banco concluída para {id_chamada} (sem transcrição)")
                # Move arquivos somente se a inserção no banco foi bem-sucedida
                caminho_destino = os.path.join(pasta_transcricoes_avaliadas, arquivo)
                shutil.copy2(caminho_transcricao, caminho_destino)
                os.remove(caminho_transcricao)
                print(f"Transcrição movida para: {caminho_destino}")
            except Exception as db_exc:
                print(f"[ERRO] Falha ao inserir avaliação no banco: {db_exc}")
                log_path = os.path.join(pasta_transcricoes_erros, f"{id_chamada}_db_erro.txt")
                with open(log_path, 'w', encoding='utf-8') as log_file:
                    log_file.write(f"Erro ao inserir avaliação no banco para {arquivo}\n")
                    log_file.write(f"Data/hora: {format_time_now()}\n")
                    log_file.write(f"Erro: {str(db_exc)}\n")
                    log_file.write(f"Conteúdo da avaliação: {json.dumps(avaliacao, ensure_ascii=False)[:2000]}\n")
                print(f"Log de erro de banco criado em: {log_path}")
                # Move a transcrição para a pasta de erros em caso de falha
                caminho_destino_erro = os.path.join(pasta_transcricoes_erros, arquivo)
                shutil.copy2(caminho_transcricao, caminho_destino_erro)
                os.remove(caminho_transcricao)
                print(f"Transcrição movida para pasta de erros: {caminho_destino_erro}")
                
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")
            try:
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
    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",  
            messages=messages,
            temperature=0.0,
            max_tokens=1024
        )

        assistant_content = response.choices[0].message.content.strip()

        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', assistant_content)
        if not json_match:
            raise ValueError(f"No JSON found in response: {assistant_content[:200]}")
        
        try:
            result = json.loads(json_match.group(0))
        except json.JSONDecodeError:
            # If direct JSON parse fails, try to clean the string first
            clean_json = assistant_content.replace('\n', ' ').replace('```json', '').replace('```', '')
            json_match = re.search(r'\{[\s\S]*\}', clean_json)
            if not json_match:
                raise ValueError(f"No JSON found in cleaned response: {clean_json[:200]}")
            result = json.loads(json_match.group(0))

        # Ensure required fields exist
        if 'id_chamada' not in result:
            result['id_chamada'] = id_chamada
        if 'itens' not in result:
            raise ValueError("Response JSON missing 'itens' field")

        # Add total field if needed
        total = result.get("pontuacao_total", 0)
        result["pontuacao_percentual"] = round((total / MAX_SEM_GSS) * 100, 1)

        print(f"Avaliação concluída para ligação: {id_chamada}")
        return result
    except Exception as e:
        error_msg = f"Erro na avaliação da ligação {id_chamada}: {str(e)}"
        print(f"ERRO: {error_msg}")
        # Return a minimal valid result structure instead of raising
        return {
            "id_chamada": id_chamada,
            "avaliador": "MonitorGPT",
            "falha_critica": True,
            "itens": {},
            "erro_processamento": str(e),
            "pontuacao_total": 0,
            "pontuacao_percentual": 0
        }

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
    subitens_validos = [s for s in subitens if s[0].strip().lower() != 'falha critica' and s[2].get('status', '').strip().upper() not in ['N/A', 'NA', 'N. A.']]
    n_validos = len(subitens_validos)
    if n_validos == 0:
        return itens
        
    peso_redistribuido = 1.0 / n_validos
    # Atribuir novo peso para cada subitem válido e zerar para N/A e Falha Critica
    for categoria, nome, info in subitens:
        if categoria.strip().lower() != 'falha critica' and info.get('status', '').strip().upper() not in ['N/A', 'NA', 'N. A.']:
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


class CarteiraConfig:
    def __init__(self, nome, pasta_audios, pasta_transcricoes, prompt_avaliacao):
        self.nome = nome
        self.pasta_audios = pasta_audios
        self.pasta_transcricoes = pasta_transcricoes
        self.prompt_avaliacao = prompt_avaliacao

class ProcessadorCarteira:
    def __init__(self, config: CarteiraConfig):
        self.config = config
        os.makedirs(self.config.pasta_audios, exist_ok=True)
        os.makedirs(self.config.pasta_transcricoes, exist_ok=True)

    def processar_audios(self):
        process_audio_folder(self.config.pasta_audios)

    def processar_transcricoes(self):
        process_transcription_folder(self.config.pasta_transcricoes)

    def gerar_relatorio(self):
        pasta_avaliacoes = os.path.join(self.config.pasta_transcricoes, 'Transcrições_avaliadas')
        csv_saida = os.path.join(pasta_avaliacoes, f'relatorio_avaliacoes_{self.config.nome}.csv')
        gerar_csv_relatorio_avaliacoes(pasta_avaliacoes, csv_saida)

    def executar(self):
        print(f'Processando carteira: {self.config.nome}')
        self.processar_audios()
        self.processar_transcricoes()
        self.gerar_relatorio()
        print(f'Processamento da carteira {self.config.nome} concluído.')

if __name__ == '__main__':
    # Configuração da carteira Águas Guariroba
    config_aguas = CarteiraConfig(
        nome='aguas_guariroba',
        pasta_audios=r'C:\Users\wanderley.terra\Documents\Audios_monitoria',
        pasta_transcricoes=os.path.join(r'C:\Users\wanderley.terra\Documents\Audios_monitoria', 'Transcrições_aguas'),
        prompt_avaliacao=SYSTEM_PROMPT
    )
    processador_aguas = ProcessadorCarteira(config_aguas)
    processador_aguas.executar()

    # Exemplo de outra carteira (descomente e ajuste para usar)
    # config_outra = CarteiraConfig(
    #     nome='outra_carteira',
    #     pasta_audios=r'C:\Users\wanderley.terra\Documents\Audios_monitoria\OutraCarteira',
    #     pasta_transcricoes=os.path.join(r'C:\Users\wanderley.terra\Documents\Audios_monitoria\OutraCarteira', 'Transcrições_outra'),
    #     prompt_avaliacao='Seu prompt específico para outra carteira aqui'
    # )
    # processador_outra = ProcessadorCarteira(config_outra)
    # processador_outra.executar()
