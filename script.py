from dask.distributed import Client

# Reemplaza <IP_DEL_SCHEDULER> con la IP pública del nodo del scheduler
client = Client('tcp://0.tcp.ngrok.io:18682')

# Verifica la conexión
print(client)
