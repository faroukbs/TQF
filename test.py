import requests
import pandas as pd
from sqlalchemy import create_engine
import os


def extract()-> dict:
    # extract data from api
    API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/coronavirus-commercants-parisiens-livraison-a-domicile/records?limit=90"
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()['results']
        return data


def transform(data:dict) -> pd.DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = pd.DataFrame(data)
    print(f"Total Number of commerces from API {len(data)}")
    df['lon'] = [foo.get('lon') for foo in df['geo_point_2d']]
    df['lat'] = [foo.get('lat') for foo in df['geo_point_2d']]
    
    return df[['nom_du_commerce', 'adresse', 'code_postal', 'type_de_commerce',
       'fabrique_a_paris', 'services', 'description', 'precisions',
       'site_internet', 'telephone', 'mail', 
       'lon', 'lat']]





def load(df:pd.DataFrame)-> None:
    """ Loads data into a sql database"""
   # get env vars
    db_user = os.environ['DB_USER'] 
    db_pass = os.environ['DB_PASSWORD']
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']

    conn_str = f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}'

    engine = create_engine(conn_str)
    # Insert the dataframe into SQL Server
    df.to_sql('commerces', engine, if_exists='replace', index=False)     # Close the database connection



data = extract()
df = transform(data)
#print(type(str(df['nom_du_commerce'])))
load(df)

