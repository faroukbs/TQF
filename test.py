import requests
import pandas as pd
from sqlalchemy import create_engine
import os

def extract() -> dict:
    # extract data from api
    API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/coronavirus-commercants-parisiens-livraison-a-domicile/records?limit=90"
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()['results']
        return data

def transform(data: dict) -> pd.DataFrame:
    """ Transforms the dataset into the desired structure and filters"""
    df = pd.DataFrame(data)
    print(f"Total Number of commerces from API {len(data)}")
    df['lon'] = [foo.get('lon') for foo in df['geo_point_2d']]
    df['lat'] = [foo.get('lat') for foo in df['geo_point_2d']]
    
    return df[['nom_du_commerce', 'adresse', 'code_postal', 'type_de_commerce',
       'fabrique_a_paris', 'services', 'description', 'precisions',
       'site_internet', 'telephone', 'mail', 
       'lon', 'lat']]

def load(df: pd.DataFrame) -> None:
    # Retrieve MySQL environment variables
    db_user = os.environ.get('MYSQL_USER')
    db_password = os.environ.get('MYSQL_PASSWORD')
    db_name = os.environ.get('MYSQL_DATABASE')

    """ Loads data into a MySQL database"""
    engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@db/{db_name}')
    # Insert the dataframe into MySQL
    df.to_sql('commerces', engine, if_exists='replace', index=False)

if __name__ == "__main__":
    data = extract()
    df = transform(data)
    load(df)
