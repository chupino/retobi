from dask.distributed import Client
import dask.dataframe as dd

# Reemplaza <IP_DEL_SCHEDULER> con la IP pública del nodo del scheduler
client = Client('tcp://4.tcp.ngrok.io:19406')

# Verifica la conexión
print(client)

df = dd.read_csv('./u.data' , delim_whitespace=True)

print(df.head())
