from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import SQLAlchemyError

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Accessing credentials and database connection parameters
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST', '127.0.0.1')  # Default to localhost if not specified
db_port = os.getenv('DB_PORT', '5432')       # Default to 5432 if not specified
db_name = os.getenv('DB_NAME', 'postgres')    # Default to 'postgres' if not specified

db_connection = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

# Function to create database engine
def create_db_engine():
    try:
        engine = create_engine(db_connection)
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Error creating database engine: {e}")
        raise

# Function to read data from a table
def read_table(engine, table_name, batch_size=None):
    try:
        if batch_size:
            # Read data in batches
            for chunk in pd.read_sql_table(table_name, engine, chunksize=batch_size):
                yield chunk
        else:
            # Read all data at once
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql(query, engine)
            return df
    except SQLAlchemyError as e:
        logging.error(f"Error reading table {table_name}: {e}")
        raise

# Main function to execute data ingestion
def main():
    engine = create_db_engine()

    # Read data from each table
    try:
        df1 = read_table(engine, "logistics")
        df2 = read_table(engine, "materials")
        df3 = read_table(engine, "projects")
        df4 = read_table(engine, "suppliers")

        # Save each dataframe to a Parquet file
        df1.to_parquet('/path/to/data/df1.parquet')
        df2.to_parquet('/path/to/data/df2.parquet')
        df3.to_parquet('/path/to/data/df3.parquet')
        df4.to_parquet('/path/to/data/df4.parquet')

        logging.info("Data ingestion and storage completed successfully.")
    except Exception as ex:
        logging.error(f"Data ingestion failed: {ex}")

if __name__ == "__main__":
    main()
