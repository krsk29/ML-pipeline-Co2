"""
This module handles the ingestion of data from a PostgreSQL database,
processes it, and stores it in Parquet format using PySpark.
"""

import os
import logging

import pandas as pd
from pyspark.sql import SparkSession

from db_utils import DBUtils
from script_utils import configure_logging
from constant_variables import DATA_INGESTED_DIR, LOGS_DIR

# Initialize Spark session
spark = SparkSession.builder.appName("Data Ingestion").getOrCreate()

# Initialise DBUtils instance
db_utils = DBUtils()

# Configuring logging to write to a file
configure_logging(LOGS_DIR, 'data_ingestion')

# Accessing credentials and database connection parameters
db_connection = db_utils.db_connection

# Main function to execute data ingestion
def main():
    """
    Main function to execute the data ingestion process.
    """
    engine = db_utils.create_db_engine()

    # Read data from each table
    try:
        # Process and save data from each table
        logistics_generator = db_utils.read_from_postgres(engine, "logistics", batch_size=20000)
        db_utils.process_and_save_batches(logistics_generator, os.path.join(DATA_INGESTED_DIR, "logistics"), spark)

        materials_generator = db_utils.read_from_postgres(engine, "materials", batch_size=20000)
        db_utils.process_and_save_batches(materials_generator, os.path.join(DATA_INGESTED_DIR, "materials"), spark)

        projects_generator = db_utils.read_from_postgres(engine, "projects", batch_size=20000)
        db_utils.process_and_save_batches(projects_generator, os.path.join(DATA_INGESTED_DIR, "projects"), spark)

        suppliers_generator = db_utils.read_from_postgres(engine, "suppliers", batch_size=20000)
        db_utils.process_and_save_batches(suppliers_generator, os.path.join(DATA_INGESTED_DIR, "suppliers"), spark)

        logging.info("Data ingestion and storage completed successfully.")

    except Exception as ex:
        logging.error(f"Data ingestion failed: {ex}")

if __name__ == "__main__":
    main()