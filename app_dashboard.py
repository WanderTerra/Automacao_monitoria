from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from datetime import datetime
from collections import defaultdict

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    'host': '10.100.10.57',
    'port': 3306,
    'user': 'user_automacao',
    'password': 'G5T82ZWMr',
    'database': 'vonix',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
}

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

@app.route('/api/dashboard')
def dashboard():
    inicio = request.args.get('inicio')
    fim = request.args.get('fim')
    carteira = request.args.get('carteira', 'AGUAS')
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    # Pontuação média e quantidade de ligações
    cursor.execute(f"""
        SELECT AVG(pontuacao) as media, COUNT(*) as qtd
        FROM avaliacoes
        WHERE data_ligacao >= %s AND data_ligacao < %s AND carteira = %s
    """, (inicio, fim, carteira))
    dash = cursor.fetchone()
    # Item com maior não conformidade
    cursor.execute(f"""
        SELECT categoria, COUNT(*) as nc
        FROM itens_avaliados ia
        JOIN avaliacoes av ON av.id = ia.avaliacao_id
        WHERE ia.resultado = 'NAO CONFORME' AND av.data_ligacao >= %s AND av.data_ligacao < %s AND av.carteira = %s
        GROUP BY categoria
        ORDER BY nc DESC LIMIT 1
    """, (inicio, fim, carteira))
    item_nc = cursor.fetchone()
    # Evolução da nota média
    cursor.execute(f"""
        SELECT DATE(data_ligacao) as dia, AVG(pontuacao) as media
        FROM avaliacoes
        WHERE data_ligacao >= %s AND data_ligacao < %s AND carteira = %s
        GROUP BY dia ORDER BY dia
    """, (inicio, fim, carteira))
    evolucao = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({
        'media': dash['media'],
        'qtd': dash['qtd'],
        'item_mais_nc': item_nc,
        'evolucao': evolucao
    })

@app.route('/api/agentes')
def agentes():
    inicio = request.args.get('inicio')
    fim = request.args.get('fim')
    carteira = request.args.get('carteira', 'AGUAS')
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"""
        SELECT av.agent_id, ag.name, AVG(av.pontuacao) as media, COUNT(*) as qtd
        FROM avaliacoes av
        JOIN agents ag ON av.agent_id = ag.id
        WHERE av.data_ligacao >= %s AND av.data_ligacao < %s AND av.carteira = %s
        GROUP BY av.agent_id, ag.name
        ORDER BY media DESC
    """, (inicio, fim, carteira))
    agentes = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(agentes)

@app.route('/api/agente/<int:agent_id>/detalhes')
def detalhes_agente(agent_id):
    inicio = request.args.get('inicio')
    fim = request.args.get('fim')
    carteira = request.args.get('carteira', 'AGUAS')
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    # Dados gerais
    cursor.execute(f"""
        SELECT ag.name, AVG(av.pontuacao) as media, COUNT(*) as qtd
        FROM avaliacoes av
        JOIN agents ag ON av.agent_id = ag.id
        WHERE av.agent_id = %s AND av.data_ligacao >= %s AND av.data_ligacao < %s AND av.carteira = %s
    """, (agent_id, inicio, fim, carteira))
    dados = cursor.fetchone()
    # Radar dos itens
    cursor.execute(f"""
        SELECT categoria, AVG(CASE WHEN resultado = 'CONFORME' THEN 1 ELSE 0 END) as taxa_conforme
        FROM itens_avaliados ia
        JOIN avaliacoes av ON av.id = ia.avaliacao_id
        WHERE av.agent_id = %s AND av.data_ligacao >= %s AND av.data_ligacao < %s AND av.carteira = %s
        GROUP BY categoria
    """, (agent_id, inicio, fim, carteira))
    radar = cursor.fetchall()
    # Evolução da nota média
    cursor.execute(f"""
        SELECT DATE(av.data_ligacao) as dia, AVG(av.pontuacao) as media
        FROM avaliacoes av
        WHERE av.agent_id = %s AND av.data_ligacao >= %s AND av.data_ligacao < %s AND av.carteira = %s
        GROUP BY dia ORDER BY dia
    """, (agent_id, inicio, fim, carteira))
    evolucao = cursor.fetchall()
    # Evolução dos itens
    cursor.execute(f"""
        SELECT categoria, DATE(av.data_ligacao) as dia,
            SUM(CASE WHEN resultado = 'CONFORME' THEN 1 ELSE 0 END) as conforme,
            SUM(CASE WHEN resultado = 'NAO CONFORME' THEN 1 ELSE 0 END) as nao_conforme
        FROM itens_avaliados ia
        JOIN avaliacoes av ON av.id = ia.avaliacao_id
        WHERE av.agent_id = %s AND av.data_ligacao >= %s AND av.data_ligacao < %s AND av.carteira = %s
        GROUP BY categoria, dia
        ORDER BY categoria, dia
    """, (agent_id, inicio, fim, carteira))
    evolucao_itens = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({
        'dados': dados,
        'radar': radar,
        'evolucao': evolucao,
        'evolucao_itens': evolucao_itens
    })

@app.route('/api/agente/<int:agent_id>/historico')
def historico_agente(agent_id):
    inicio = request.args.get('inicio')
    fim = request.args.get('fim')
    carteira = request.args.get('carteira', 'AGUAS')
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"""
        SELECT av.id as avaliacao_id, av.data_ligacao, av.pontuacao, av.status_avaliacao, ia.categoria, ia.resultado, ia.descricao
        FROM avaliacoes av
        JOIN itens_avaliados ia ON av.id = ia.avaliacao_id
        WHERE av.agent_id = %s AND av.data_ligacao >= %s AND av.data_ligacao < %s AND av.carteira = %s
        ORDER BY av.data_ligacao DESC, av.id, ia.categoria
    """, (agent_id, inicio, fim, carteira))
    historico = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(historico)

@app.route('/api/transcricao/<int:avaliacao_id>')
def transcricao(avaliacao_id):
    conn = get_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT conteudo FROM transcricoes WHERE avaliacao_id = %s", (avaliacao_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return jsonify({'conteudo': row['conteudo'] if row else ''})

if __name__ == '__main__':
    app.run(debug=True)
