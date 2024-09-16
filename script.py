from dask.distributed import Client
import dask.dataframe as dd
import fsspec

client = Client('tcp://0.tcp.ngrok.io:15644')
print(client)

# URLs de los archivos
ratings_url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.data'
users_url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.user'
movies_url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.item'

# Leer los archivos con Dask, especificando codificación y delimitadores
ratings_df = dd.read_csv(
    ratings_url,
    sep='\s+',
    names=['user_id', 'item_id', 'rating', 'timestamp'],
    encoding='ISO-8859-1',
    assume_missing=True
)

users_df = dd.read_csv(
    users_url,
    sep='|',
    names=['user_id', 'age', 'gender', 'occupation', 'zip_code'],
    encoding='ISO-8859-1',
    assume_missing=True
)

# Definir los nombres de las columnas para movies.data
genres_columns = [
    'movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL',
    'unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy', 'Crime',
    'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
    'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
]

movies_df = dd.read_csv(
    movies_url,
    sep='|',
    names=genres_columns,
    encoding='ISO-8859-1',
    assume_missing=True
)

# Repartir los DataFrames para aprovechar el clúster
num_partitions = 10
ratings_df = ratings_df.repartition(npartitions=num_partitions)
users_df = users_df.repartition(npartitions=num_partitions)
movies_df = movies_df.repartition(npartitions=num_partitions)

# Unir DataFrames
merged_df = ratings_df.merge(users_df, on='user_id').merge(movies_df, left_on='item_id', right_on='movie_id')

# Agrupar por edad y calcular la preferencia de géneros
genres = [
    'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Drama', 'Fantasy', 
    'Horror', 'Musical', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
]

genre_by_age = merged_df.groupby('age')[genres].mean().compute()

# Mostrar preferencias de género por edad
for age, data in genre_by_age.iterrows():
    print(f"Edad: {age}")
    print(data)
    print("\n")
