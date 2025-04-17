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
    """Processa um arquivo de áudio: transcrição em VTT e diarização com Pyannote.audio."""
    # 1. Transcreve o áudio com Whisper e solicita saída em VTT (com timestamps)
    print(f"Transcrevendo (VTT) {caminho_audio}...")
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        with open(caminho_audio, 'rb') as audio_file:
            transcription_response = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                response_format="vtt"
            )
        
        # A resposta pode vir em diferentes formatos, dependendo da versão da API
        if hasattr(transcription_response, 'text'):
            vtt_text = transcription_response.text
        elif isinstance(transcription_response, str):
            vtt_text = transcription_response
        elif isinstance(transcription_response, dict) and "text" in transcription_response:
            vtt_text = transcription_response["text"]
        else:
            print(f"Formato de resposta desconhecido: {type(transcription_response)}")
            print(f"Conteúdo: {transcription_response}")
            return None
        
        print("Transcrição concluída!")
    except Exception as e:
        print(f"Erro na transcrição: {e}")
        return None

    # Processa a transcrição VTT para texto com timestamps (formato padronizado)
    segments = parse_vtt(vtt_text)
    formatted_text = []
    for seg in segments:
        start_time = seg["start"]
        end_time = seg["end"]
        # Formata os tempos em hh:mm:ss
        def format_time(s):
            hrs = int(s // 3600)
            mins = int((s % 3600) // 60)
            secs = s % 60
            return f"{hrs:02d}:{mins:02d}:{secs:05.2f}"
        time_str = f"[{format_time(start_time)} - {format_time(end_time)}]"
        formatted_text.append(f"{time_str} SPEAKER_UNKNOWN: {seg['text']}")
    
    formatted_transcription = "\n".join(formatted_text)
    
    # 2. Tenta realizar a diarização com Pyannote.audio
    diarization_success = False
    if pipeline is not None:
        print(f"Realizando diarização em {caminho_audio}...")
        try:
            diarization = pipeline(caminho_audio)
            print("Diarização concluída!")
            
            # Mescla a transcrição com a diarização
            print("Mesclando transcrição e diarização...")
            final_text = merge_transcript_and_diarization(vtt_text, diarization)
            diarization_success = True
        except Exception as e:
            print(f"Erro na diarização: {e}")
            diarization_success = False
    
    # 3. Se a diarização falhar ou não estiver disponível, use GPT-4.1-mini
    if not diarization_success:
        print("Usando GPT-4.1-mini para identificar falantes...")
        final_text = classificar_falantes_com_gpt(formatted_transcription)
    
    return final_text

def process_audio_folder(pasta):
    extensoes_audio = ['.mp3', '.wav', '.m4a', '.ogg', '.flac']
    arquivos = [f for f in os.listdir(pasta) if os.path.splitext(f)[1].lower() in extensoes_audio]
    if not arquivos:
        print('Nenhum arquivo de áudio encontrado na pasta.')
        return
    
    # Criar pastas para áudios processados e transcrições
    pasta_audios_transcritos = os.path.join(pasta, 'Audios_transcritos')
    pasta_transcricoes = os.path.join(pasta, 'Transcrições_aguas')
    
    # Criar as pastas se não existirem
    os.makedirs(pasta_audios_transcritos, exist_ok=True)
    os.makedirs(pasta_transcricoes, exist_ok=True)
    
    print(f"Pasta para áudios processados: {pasta_audios_transcritos}")
    print(f"Pasta para transcrições: {pasta_transcricoes}")
    
    for arquivo in arquivos:
        caminho_audio = os.path.join(pasta, arquivo)
        print(f"Processando: {arquivo}")
        final_text = process_audio_file(caminho_audio)
        
        if final_text:
            nome_txt = os.path.splitext(arquivo)[0] + '_diarizado.txt'
            caminho_txt = os.path.join(pasta_transcricoes, nome_txt)
            
            # Salvar a transcrição na pasta de transcrições
            with open(caminho_txt, 'w', encoding='utf-8') as f:
                f.write(final_text)
            print(f"Transcrição salva em: {caminho_txt}")
            
            # Mover o áudio processado para a pasta de áudios processados
            caminho_destino = os.path.join(pasta_audios_transcritos, arquivo)
            try:
                import shutil
                shutil.move(caminho_audio, caminho_destino)
                print(f"Arquivo de áudio movido para: {caminho_destino}")
            except Exception as e:
                print(f"Erro ao mover o arquivo de áudio: {e}")
        else:
            print(f"Falha ao processar {arquivo}")

if __name__ == '__main__':
    pasta_audios = r'C:\Users\wanderley.terra\Documents\Audios_monitoria'
    process_audio_folder(pasta_audios)
