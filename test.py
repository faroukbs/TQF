import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import pymysql
import matplotlib.pyplot as plt


db_user = os.environ.get('DB_USER')
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
db_password = os.environ.get('DB_PASSWORD')

def clean_text(text):
    # Remove non-UTF-8 characters
    return ''.join(filter(lambda char: ord(char) < 128, text))

def extract() -> list:
    API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/coronavirus-commercants-parisiens-livraison-a-domicile/records"
    limit = 100  # Set the limit per request
    offset = 0   # start from 
    all_records = []

    while offset < 2503:
        params = {"limit": limit, "offset": offset}
        response = requests.get(API_URL, params=params)

        if response.status_code == 200:
            data = response.json().get("results", [])
            all_records.extend(data)
            offset += limit
        else:
            print("Server is Down. Try again later.")
            break
    
    return all_records

def create_nom_du_commerce_table():
    try:
        # Connect to the MySQL server localy
       #db_user = os.environ.get('root')
       #db_host = os.environ.get('localhost:3306')
       #db_name = os.environ.get('commerces')
       #db_password = os.environ.get('')
 
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            passwd=db_password,
            db=db_name  # Use the existing database
        )
        
        cursor = connection.cursor()

        # Create a table for 'nom_du_commerce' with a unique ID
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nom_du_commerce (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            UNIQUE(name)
        )
        """
        cursor.execute(create_table_query)

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print('Table creation for nom_du_commerce failed:', e)

def create_code_postal_table():
    try:
        # Connect to the MySQL server localy
       # db_user = os.environ.get('root')
       # db_host = os.environ.get('localhost:3306')
       # db_name = os.environ.get('commerces')
       # db_password = os.environ.get('')
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            passwd=db_password,
            db=db_name
        )
        
        cursor = connection.cursor()

        # Create a table for code_postal with a unique ID
        create_table_query = """
        CREATE TABLE IF NOT EXISTS code_postal (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code_postal VARCHAR(255) NOT NULL,
            UNIQUE(code_postal)
        )
        """
        cursor.execute(create_table_query)

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print('Table creation for code_postal failed:', e)

def extract_lon(geo_point):
    return geo_point.get("lon")

def extract_lat(geo_point):
    return geo_point.get("lat")

def transform(data: dict) -> pd.DataFrame:
    try:
        df = pd.DataFrame(data)

        # Extract 'lon' and 'lat' using the modular functions
        df["lon"] = df["geo_point_2d"].apply(extract_lon)
        df["lat"] = df["geo_point_2d"].apply(extract_lat)

        # Select only the desired columns
        columns_to_keep = [
            "nom_du_commerce",
            "adresse",
            "code_postal",
            "type_de_commerce",
            "fabrique_a_paris",
            "services",
            "description",
            "precisions",
            "site_internet",
            "telephone",
            "mail",
            "lon",
            "lat",
        ]

        df = df[columns_to_keep]
       # Handle missing values
        df.fillna({'nom_du_commerce_id': 'Not Available','adresse': 'Not Available','code_postal': 'Not Available','type_de_commerce': 'Not Available','fabrique_a_paris': 'Not Available','services': 'Not Available','description': 'Not Available','precisions': 'Not Available','site_internet': 'Not Available', 'telephone': 'Not Available', 'mail': 'Not Available'}, inplace=True)
        df = df.replace('<Nul>', 'Not Available')
        # Remove duplicate rows if they exist
        df.drop_duplicates(inplace=True)
            # Clean the 'nom_du_commerce' column
        df['nom_du_commerce'] = df['nom_du_commerce'].apply(clean_text)
        # Create a mapping of nom du commerce to their IDs
        nom_du_commerce_mapping = {}
        for index, row in df.iterrows():
            name = row['nom_du_commerce'] 
            if name not in nom_du_commerce_mapping:
                nom_du_commerce_mapping[name] = len(nom_du_commerce_mapping) + 1

        # Add a new column 'nom_du_commerce_id' to 'df' using the mapping
        df['nom_du_commerce_id'] = df['nom_du_commerce'].map(nom_du_commerce_mapping)

         # Create a mapping of postal code to their IDs
        code_mapping = {}
        for index, row in df.iterrows():
            name =  row['code_postal']
            if name not in code_mapping:
                code_mapping[name] = len(code_mapping) + 1

        # Add a new column code_postal_id to 'df' using the mapping
        df['code_postal_id'] = df['code_postal'].map(code_mapping)
        # Reorder the columns to have code_postal_id as the second column
        df = df[['nom_du_commerce_id'] +['code_postal_id']+ columns_to_keep]
        print(f"Total Number of commerces from API {len(data)}")

        return df
    except Exception as e:
        raise Exception("Data transformation failed: " + str(e))
    
def create_database_if_not_exists():
    try:
        db_user = os.environ.get('DB_USER')
        db_host = os.environ.get('DB_HOST')
        db_name = os.environ.get('DB_NAME')
        db_password = os.environ.get('DB_PASSWORD')

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

        # Loads data into a MySQL database localy
        #engine = create_engine('mysql+pymysql://root:@localhost:3306/commerces')
        
        #df.to_sql('commerces', engine, if_exists="replace", index=False)
        # Loads data into a MySQL database in the container
        engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@mysql/{db_name}")

                # Create tables
        create_nom_du_commerce_table()
        create_code_postal_table()

        df.to_sql('commerces', engine, if_exists="replace", index=False)
        # Insert the dataframe into MySQL
        #df.to_sql(db_name, engine, if_exists="replace", index=False)
          # Insert the 'nom_du_commerce' data into the 'nom_du_commerce' table
        nom_du_commerce_df = df[['nom_du_commerce_id', 'nom_du_commerce']].drop_duplicates()
        nom_du_commerce_df.to_sql('nom_du_commerce', engine, if_exists="replace", index=False)

        # Remove the 'nom_du_commerce' column from the main dataframe
        df.drop(columns=['nom_du_commerce'], inplace=True)

                  # Insert the 'nom_du_commerce' data into the 'nom_du_commerce' table
        code_postal_df = df[['code_postal_id', 'code_postal']].drop_duplicates()
        code_postal_df.to_sql('code_postal', engine, if_exists="replace", index=False)

        # Remove the 'nom_du_commerce' column from the main dataframe
        df.drop(columns=['code_postal'], inplace=True)
        # Insert the remaining data into the 'commerces' table
        
    except Exception as e:
        print("Data can't be loaded:", e)

def generate_type_of_commerce_chart():
    # Connect to your MySQL database
    connection = pymysql.connect(
      host=db_host,
      user=db_user,
      passwd=db_password,
      db=db_name  # Use the existing database
        )

    # Create a cursor
    cursor = connection.cursor()

    # Execute SQL query to get the distribution of orders by type of commerce
    query = "SELECT type_de_commerce, COUNT(*) AS nbr_commande FROM commerces GROUP BY type_de_commerce"
    cursor.execute(query)

    # Fetch the results
    results = cursor.fetchall()

    # Extract data for the chart
    commerce_types = [row[0] for row in results]
    nbr_commande = [row[1] for row in results]

    # Create a bar chart for order distribution by type of commerce
    plt.figure(figsize=(10, 6))
    plt.bar(commerce_types, nbr_commande)
    plt.title('Répartition des commandes par type de commerce')
    plt.xlabel('Type de Commerce')
    plt.ylabel('Nombres des Orders')
    plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
    plt.tight_layout()

    # Save the chart as an image file
    chart_image_filename = "chart.png"
    plt.savefig(chart_image_filename, bbox_inches='tight')
    print(f"Chart saved as {chart_image_filename}")

    # Show the chart
    plt.show()

    # Close the cursor and connection
    cursor.close()
    connection.close()


if __name__ == "__main__":
    data = extract()
    df = transform(data)
    create_database_if_not_exists()
    # Iterate over DataFrame columns with string data and clean them
    string_columns = ['nom_du_commerce', 'adresse', 'type_de_commerce', 'fabrique_a_paris', 'services', 'description', 'precisions', 'site_internet', 'telephone', 'mail']
    for col in string_columns:
        df[col] = df[col].apply(clean_text)

    load(df)
#    generate_type_of_commerce_chart()