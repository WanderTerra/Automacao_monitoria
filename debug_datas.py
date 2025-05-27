from datetime import datetime, timedelta

hoje = datetime.now()
print('Hoje:', hoje, 'weekday:', hoje.weekday())
if hoje.weekday() == 0:
    dia_busca = hoje - timedelta(days=2)
else:
    dia_busca = hoje - timedelta(days=1)
print('Dia buscado:', dia_busca)
data_inicio = dia_busca.replace(hour=0, minute=0, second=0, microsecond=0)
data_fim = data_inicio + timedelta(days=1)
print('Data início:', data_inicio)
print('Data fim:', data_fim)
