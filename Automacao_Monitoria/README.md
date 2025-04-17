# Backend de Transcrição de Áudio

Este projeto é um backend em Python usando Flask para transcrever áudios via API da OpenAI.

## Como usar

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Defina a variável de ambiente `OPENAI_API_KEY` com sua chave da OpenAI.
3. Rode o servidor:
   ```bash
   python app.py
   ```
4. Faça uma requisição POST para `/transcribe` enviando um arquivo de áudio no campo `audio`.

## Exemplo de requisição (usando curl):
```bash
curl -X POST -F "audio=@seuarquivo.mp3" http://localhost:5000/transcribe
```
