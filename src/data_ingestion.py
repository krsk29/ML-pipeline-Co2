"""
This module handles the ingestion of data from a PostgreSQL database,
processes it, and stores it in Parquet format using PySpark.
"""

import os
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pyspark.sql import SparkSession
from dotenv import load_dotenv


# Initialize Spark session
spark = SparkSession.builder.appName("Data Ingestion").getOrCreate()

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
    """
    Create and return a database engine.

    :return: A SQLAlchemy engine connected to the database.
    """
    try:
        engine = create_engine(db_connection)
        return engine
    except SQLAlchemyError as e:
        logging.error(f"Error creating database engine: {e}")
        raise

# Function to read data from a table
def read_table(engine, table_name, batch_size=None):
    """
    Read data from a database table.

    :param engine: The database engine.
    :param table_name: The name of the table to read.
    :param batch_size: The size of the batch to read at a time (for large tables).
    :return: A generator yielding data batches as DataFrames (if batch_size is set), or a single DataFrame.
    """
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

def standardize_dates(batch_df):
    """
    Standardize the format of date columns in a DataFrame.

    :param batch_df: The DataFrame whose date columns are to be standardized.
    :return: The DataFrame with standardized date formats.
    """
    # List of all possible date columns
    date_columns = ['transaction_date', 'project_start_date', 'project_end_date']

    for col in date_columns:
        if col in batch_df.columns:
            batch_df[col] = pd.to_datetime(batch_df[col]).dt.strftime('%Y-%m-%d')
    return batch_df

#fucntion to save batch data to parquet
def process_and_save_batches(generator, base_file_path):
    """
    Process and save each batch of data from a generator to Parquet format.

    :param generator: A generator yielding data batches.
    :param base_file_path: The base file path for storing Parquet files.
    """
    for i, batch in enumerate(generator):
        batch = standardize_dates(batch)  # Standardize dates
        batch_file_path = f"{base_file_path}_{i}.parquet"
        
        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(batch)
        
        # Save as Parquet using Spark
        spark_df.write.mode("overwrite").parquet(batch_file_path)

# Main function to execute data ingestion
def main():
    """
    Main function to execute the data ingestion process.
    """
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
