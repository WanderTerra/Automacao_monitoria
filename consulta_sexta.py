import mysql.connector

DB_CONFIG = {
    'host': '10.100.10.57',
    'port': 3306,
    'user': 'user_automacao',
    'password': 'G5T82ZWMr',
    'database': 'vonix',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
}

SQL = """
SELECT call_id, queue_id, start_time, answer_time, hangup_time, call_secs
FROM vonix.calls AS c
WHERE queue_id LIKE 'aguas%'
  AND queue_id NOT LIKE 'aguasguariroba%'
  AND status LIKE 'Completada%'
  AND start_time >= '2025-05-16 00:00:00'
  AND start_time < '2025-05-18 00:00:00'
  AND call_secs > 60
ORDER BY start_time DESC
"""

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(SQL)
    rows = cursor.fetchall()
    print(f"{len(rows)} registros encontrados.")
    for row in rows:
        print(row)
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Erro ao consultar banco: {e}")
