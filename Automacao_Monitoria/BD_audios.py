import mysql.connector

# Dados da conexão
config = {
    'host': '10.100.10.57',
    'port': 3306,
    'user': 'user_automacao',
    'password': 'G5T82ZWMr',
    'database': 'vonix',
    'collation': 'utf8mb4_unicode_ci'
}

try:
    # Conecta ao banco
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Consulta SQL
    query = """
    SELECT call_id FROM vonix.calls AS c
    WHERE status = 'Completada'
    ORDER BY start_time DESC
    """

    # Executa a consulta
    cursor.execute(query)

    # Busca os resultados
    results = cursor.fetchall()
    for row in results:
        print("call_id:", row[0])

except mysql.connector.Error as err:
    print("Erro ao conectar ou executar consulta:", err)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and conn.is_connected():
        conn.close()
