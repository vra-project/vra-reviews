'''
Programa utilizado para obtener las reviews de la pagina web RAWG.io
- Se extraen los datos necesarios de las reviews
- Se generan ficheros de 50000 reviews
- Se cargan en un bucket de S3
'''

# %%
# Se cargan las librerias necesarias
from math import ceil
from configparser import ConfigParser
import warnings
import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from bs4 import BeautifulSoup
import boto3

# %%
# Se cargan las credenciales de AWS

config = ConfigParser()
config.read('secrets.toml', encoding='utf-8')

BUCKET_S3 = config['AWS']['bucket_s3']
FOLDER = 'reviews/'

REVIEW_URL = 'https://rawg.io/api/reviews'
N_REVIEWS = 50000

warnings.filterwarnings('ignore')

# %%
# Se define una sesion para realizar una serie de reintentos en caso de fallos
# a la hora de consultar las distintas urls utilizadas
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.2,
    status_forcelist=[500, 502, 503, 504]
)
session.mount('https://', HTTPAdapter(max_retries=retries))

# %%
# Se conecta con S3
bucket = (
    boto3.resource(
        's3',
        region_name='us-east-1'
        )
    .Bucket(name=BUCKET_S3[5:])
    )

print('Conectado a S3')

# %%
# Se comprueban los archivos de reviews ya disponibles
# En caso de no haber ninguno, se comenzara la extraccion desde la primera
# review disponible
av_files = [
    obj.key for obj in bucket.objects.filter(Prefix=FOLDER)
    if len(obj.key) > len(FOLDER)
    ]

if len(av_files) == 0:
    start_review = (
        session
        .get(f'{REVIEW_URL}?ordering=id')
        .json()['results'][0]['id']
        )
    APPEND_FIRST = False
else:
    final_review_file = av_files[-1]
    final_file = pd.read_feather(f'{BUCKET_S3}/{final_review_file}')
    start_review = int(final_file.iloc[-1]['id']+1)
    APPEND_FIRST = True

print(f'Primera review: {start_review}')

# %%
# Se obtiene la ultima review disponible en RAWG
final_review = (
    session
    .get(f'{REVIEW_URL}?ordering=-id')
    .json()['results'][0]['id']
    )

print(f'Ultima review: {final_review}')

# %%
# Se obtiene la informacion relevante de todas las reviews
# Se crearan archivos cada 50000 reviews

reviews = []
for review_id in range(start_review, final_review+1):
    try:
        review = session.get(f'{REVIEW_URL}/{review_id}').json()
        reviews.append({
            'id': review_id,
            'user_id': review['user']['slug'],
            'game_id': review['game']['slug'],
            'review_text': BeautifulSoup(review['text']).get_text(),
            'review_rating': review['rating']
            })
    except:
        pass
    if review_id % N_REVIEWS == 0 or review_id == final_review:
        reviews_df = pd.DataFrame.from_records(reviews)
        if APPEND_FIRST:
            reviews_df = (
                pd.concat([final_file, reviews_df])
                .reset_index(drop=True)
                .astype(str)
                )
            APPEND_FIRST = False
        top_name = int(ceil(review_id/N_REVIEWS)*N_REVIEWS)
        low_name = top_name - (N_REVIEWS-1)
        reviews_df.to_feather(
            (
                f'{BUCKET_S3}/{FOLDER}reviews_{low_name:07d}_{top_name:07d}'
                '.feather'
                ),
            compression='lz4'
            )
        print(f'reviews_{low_name}_{top_name} creado')
        reviews = []
