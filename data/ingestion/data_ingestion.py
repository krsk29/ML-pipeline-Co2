from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import SQLAlchemyError

# Function to safely get environment variable
def get_env_variable(var_name, default=None):
    value = os.getenv(var_name, default)
    if value is None:
        raise ValueError(f"{var_name} environment variable not set")
    return value

# Load environment variables from .env file
load_dotenv()

# Accessing environment variables with defensive checks
logs_dir = get_env_variable('LOGS_DIR')
data_ingested_dir = get_env_variable('DATA_INGESTED_DIR')
db_username = get_env_variable('DB_USERNAME')
db_password = get_env_variable('DB_PASSWORD')
db_host = get_env_variable('DB_HOST', '127.0.0.1')
db_port = get_env_variable('DB_PORT', '5432')
db_name = get_env_variable('DB_NAME', 'postgres')

# Configuring logging to write to a file
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f'{logs_dir}/data_ingestion_{current_time}.log'
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Accessing credentials and database connection parameters
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

#fucntion to save batch data to parquet
def process_and_save_batches(generator, base_file_path):
    for i, batch in enumerate(generator):
        batch_file_path = f"{base_file_path}_{i}.parquet"
        batch.to_parquet(batch_file_path)

# Main function to execute data ingestion
def main():
    engine = create_db_engine()

    # Read data from each table
    try:
        # Process and save data from each table
        logistics_generator = read_table(engine, "logistics", batch_size=20000)
        process_and_save_batches(logistics_generator, os.path.join(data_ingested_dir, "logistics"))

        materials_generator = read_table(engine, "materials", batch_size=20000)
        process_and_save_batches(materials_generator, os.path.join(data_ingested_dir, "materials"))

        projects_generator = read_table(engine, "projects", batch_size=20000)
        process_and_save_batches(projects_generator, os.path.join(data_ingested_dir, "projects"))

        suppliers_generator = read_table(engine, "suppliers", batch_size=20000)
        process_and_save_batches(suppliers_generator, os.path.join(data_ingested_dir, "suppliers"))

        logging.info("Data ingestion and storage completed successfully.")

    except Exception as ex:
        logging.error(f"Data ingestion failed: {ex}")

if __name__ == "__main__":
    main()
