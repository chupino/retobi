from dask.distributed import Client
import dask.dataframe as dd
import fsspec
import pandas as pd
from io import StringIO
import time

client = Client('tcp://0.tcp.ngrok.io:15644')
print(client)

url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.data'

fs = fsspec.filesystem('http')
with fs.open(url) as file:
    content = file.read()
    content = content.decode('utf-8')
    data = StringIO(content)
    df_pandas = pd.read_csv(data, sep='\s+')
    df = dd.from_pandas(df_pandas, npartitions=1)

print(df.head())

start_time = time.time()

first_column_name = df.columns[0]
mean_value = df[first_column_name].mean().compute()

end_time = time.time()
execution_time = end_time - start_time

print(f"La media de la primera columna es: {mean_value}")
print(f"Tiempo de ejecución: {execution_time} segundos")
