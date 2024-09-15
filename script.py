from dask.distributed import Client
import dask.dataframe as dd
import urllib.request
import os


# Reemplaza <IP_DEL_SCHEDULER> con la IP pública del nodo del scheduler
client = Client('tcp://8.tcp.ngrok.io:19538')

# Verifica la conexión
print(client)

# Define la URL del archivo y la ruta local para guardarlo
url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.data'
local_filename = 'u.data'

# Descargar el archivo
urllib.request.urlretrieve(url, local_filename)

# Leer el archivo en un DataFrame de Dask
df = dd.read_csv(local_filename, delim_whitespace=True)

# Mostrar las primeras filas del DataFrame
print(df.head())
