import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from constant_variables import *

class DBUtils:
    def __init__(self):
        # Set up database connection parameters
        self.db_username = DB_USERNAME
        self.db_password = DB_PASSWORD
        self.db_host = DB_HOST
        self.db_port = DB_PORT
        self.db_name = DB_NAME
        self.jdbc_driver = JDBC_DRIVER

        # SQLAlchemy DB connection string
        self.db_connection = f'postgresql://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'

        # JDBC URL and connection properties for Spark
        self.jdbc_url = f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"
        self.properties = {
            "user": self.db_username,
            "password": self.db_password,
            "driver": "org.postgresql.Driver"
        }

    def create_db_engine(self):
        """Create and return a SQLAlchemy database engine."""
        try:
            engine = create_engine(self.db_connection)
            return engine
        except SQLAlchemyError as e:
            logging.error(f"Error creating database engine: {e}")
            raise

    def read_from_postgres(self, engine, table_name, batch_size=None):
        """
        Read data from a PostgreSQL table using SQLAlchemy.

        :param engine: The SQLAlchemy database engine.
        :param table_name: The name of the table to read.
        :param batch_size: Optional batch size for reading large tables.
        :return: DataFrame containing the table data.
        """
        try:
            if batch_size:
                # Yield chunks as DataFrames for large tables
                for chunk in pd.read_sql_table(table_name, engine, chunksize=batch_size):
                    yield chunk
            else:
                # Read entire table at once
                df = pd.read_sql_table(table_name, engine)
                return df
        except SQLAlchemyError as e:
            logging.error(f"Error reading table {table_name}: {e}")
            raise

    def write_to_postgres(self, df, table_name: str, mode: str = "overwrite"):
        """
        Write a Spark DataFrame to a PostgreSQL table using JDBC.

        :param df: Spark DataFrame to write.
        :param table_name: Target table name.
        :param mode: Write mode ('overwrite', 'append', etc.).
        """
        df.write.jdbc(url=self.jdbc_url, table=table_name, mode=mode, properties=self.properties)

    def _standardize_dates(self, batch_df):
        """
        Standardize the format of date columns in a DataFrame.

        :param batch_df: The DataFrame whose date columns are to be standardized.
        :return: The DataFrame with standardized date formats.
        """
        date_columns = DATE_COLUMNS

        for col in date_columns:
            if col in batch_df.columns:
                batch_df[col] = pd.to_datetime(batch_df[col]).dt.strftime('%Y-%m-%d')
        return batch_df

    def process_and_save_batches(self, generator, base_file_path, spark_session):
        """
        Process and save each batch of data from a generator to Parquet format.

        :param generator: A generator yielding data batches.
        :param base_file_path: The base file path for storing Parquet files.
        :param spark_session: The active Spark session.
        """
        for i, batch in enumerate(generator):
            batch = self._standardize_dates(batch)  # Standardize dates
            batch_file_path = f"{base_file_path}_{i}.parquet"
            
            # Convert Pandas DataFrame to Spark DataFrame
            spark_df = spark_session.createDataFrame(batch)
            
            # Save as Parquet using Spark
            spark_df.write.mode("overwrite").parquet(batch_file_path)