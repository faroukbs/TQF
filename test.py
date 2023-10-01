import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import pymysql


def extract() -> dict: 
    # extract data from api
    API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/coronavirus-commercants-parisiens-livraison-a-domicile/records?limit=90"
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()['results']
        return data
    else:
        print("Server is Down try it later")

def transform(data: dict) -> pd.DataFrame:
    """ Transforms the dataset into the desired structure and filters"""
    try:
        df = pd.DataFrame(data)
        print(f"Total Number of commerces from API {len(data)}")

        # Filter the dataset to keep only the columns we need
        def extract_lon(geo_point):
         return geo_point.get('lon')

        def extract_lat(geo_point):
            return geo_point.get('lat')

        # Use apply to extract 'lon' and 'lat' one by one
        df['lon'] = df['geo_point_2d'].apply(extract_lon)
        df['lat'] = df['geo_point_2d'].apply(extract_lat)
    
        return df[['nom_du_commerce', 'adresse', 'code_postal', 'type_de_commerce',
        'fabrique_a_paris', 'services', 'description', 'precisions',
        'site_internet', 'telephone', 'mail', 
        'lon', 'lat']]
    except Exception as e:
        print('data can\'t be transformed',e)

def create_database_if_not_exists():
    try:
        db_user = os.environ.get('MYSQL_USER')
        db_host = os.environ.get('MYSQL_HOST')
        db_name = os.environ.get('MYSQL_DATABASE')
        db_password = os.environ.get('MYSQL_PASSWORD')

        # Connect to MySQL server
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            passwd=db_password
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Execute the SQL command to create the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        # Close the cursor and the connection
        cursor.close()
        connection.close()

    except Exception as e:
        print('Database creation failed:', e)
    
def load(df: pd.DataFrame) -> None:
    try:
        # Retrieve MySQL environment variables from the docker compose 
        db_user = os.environ.get('MYSQL_USER')
        db_password = os.environ.get('MYSQL_PASSWORD')
        db_name = os.environ.get('MYSQL_DATABASE')
        

        # Loads data into a MySQL database localy
        #engine = create_engine('mysql+pymysql://root:@localhost:3306/commerces')

        #Loads data into a MySQL database in the container
        engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@db/{db_name}')

        # Insert the dataframe into MySQL
        df.to_sql(db_name, engine, if_exists='replace', index=False)
    except Exception as e:
        print('Data can\'t be loaded:', e) 

if __name__ == "__main__":
    data = extract()
    df = transform(data)
    create_database_if_not_exists()
    load(df)
