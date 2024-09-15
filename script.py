from dask.distributed import Client

# Reemplaza <IP_DEL_SCHEDULER> con la IP pública del nodo del scheduler
client = Client('tcp://2.tcp.ngrok.io:19512')

# Verifica la conexión
print(client)
