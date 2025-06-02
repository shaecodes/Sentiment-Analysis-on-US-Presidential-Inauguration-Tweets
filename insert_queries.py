from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASS')

try:
    tweets = pd.read_csv('data/tweets.csv')
    user = pd.read_csv('data/user.csv')
    country = pd.read_csv('data/country.csv')
except FileNotFoundError as e:
    print(f"CSV file not found: {e}")
    exit()

property_table = [
    ('Tweets', tweets),
    ('Users', user),
    ('Countries', country)
]
connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(connection_string)

for table_name, df in property_table:
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Inserted into {table_name} successfully.")
    except SQLAlchemyError as e:
        print(f"Error inserting into {table_name}: {e}")
        engine.dispose()
        break  

print("Script completed.")
