from dask.distributed import Client
import dask.dataframe as dd
import fsspec
import aiohttp
import asyncio

async def fetch_file(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            return content

async def fetch_all_files():
    ratings_url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.data'
    users_url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.user'
    movies_url = 'https://raw.githubusercontent.com/chupino/retobi/main/u.item'
    
    ratings_content, users_content, movies_content = await asyncio.gather(
        fetch_file(ratings_url),
        fetch_file(users_url),
        fetch_file(movies_url)
    )
    return ratings_content, users_content, movies_content

def process_data():
    client = Client('tcp://4.tcp.ngrok.io:13762')
    print(client)

    # Definir columnas para los DataFrames
    ratings_columns = ['user_id', 'item_id', 'rating', 'timestamp']
    users_columns = ['user_id', 'age', 'gender', 'occupation', 'zip_code']
    genres_columns = [
        'movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL',
        'unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy', 'Crime',
        'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
        'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
    ]
    
    # Ejecutar la función asincrónica para obtener el contenido de los archivos
    loop = asyncio.get_event_loop()
    ratings_content, users_content, movies_content = loop.run_until_complete(fetch_all_files())

    # Leer datos usando Dask
    ratings_df = dd.read_csv(
        StringIO(ratings_content),
        sep='\s+',
        names=ratings_columns,
        encoding='ISO-8859-1',
        assume_missing=True
    )

    users_df = dd.read_csv(
        StringIO(users_content),
        sep='|',
        names=users_columns,
        encoding='ISO-8859-1',
        assume_missing=True
    )

    movies_df = dd.read_csv(
        StringIO(movies_content),
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

# Ejecutar la función principal
process_data()


