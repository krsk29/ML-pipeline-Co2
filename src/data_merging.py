"""
This module is for merging various data sources into a single dataset and
writing the results to a PostgreSQL database.
"""
import os
import logging

from pyspark.sql import SparkSession

from constant_variables import LOGS_DIR
from db_utils import DBUtils
from script_utils import configure_logging, read_parquet

# Initialise DBUtils instance
db_utils = DBUtils()

# Configuring logging to write to a file
configure_logging(LOGS_DIR, 'data_merging')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Merging") \
    .config("spark.jars", db_utils.jdbc_driver) \
    .getOrCreate()

# Function to perform data transformations
def transform_data(data_ingested_dir, data_preprocessed_dir, jdbc_url, properties):
    """
    Perform data transformations and write the result to PostgreSQL.

    :param spark: Spark session object.
    :param data_ingested_dir: Directory where ingested data is stored.
    :param jdbc_url: JDBC URL for the PostgreSQL database.
    :param properties: Connection properties for the database.
    :return: Final transformed DataFrame.
    """
    # Read ingested data
    logistics_df = read_parquet(os.path.join(data_ingested_dir, "logistics_*.parquet"), spark)
    materials_df = read_parquet(os.path.join(data_ingested_dir, "materials_*.parquet"), spark)
    projects_df = read_parquet(os.path.join(data_ingested_dir, "projects_*.parquet"), spark)
    suppliers_df = read_parquet(os.path.join(data_ingested_dir, "suppliers_*.parquet"), spark)

    # Handling null values and dropping columns
    # For 'distance_covered' and 'CO2_emission' in logistics data
    distance_covered_median = logistics_df.approxQuantile("distance_covered", [0.5], 0)[0]
    co2_median = logistics_df.approxQuantile("CO2_emission", [0.5], 0)[0]
    logistics_df = logistics_df.fillna({"distance_covered": distance_covered_median, "CO2_emission": co2_median})
    logistics_df = logistics_df.drop("supplier_rating")

    # For 'project_budget' in projects data
    project_budget_median = projects_df.approxQuantile("project_budget", [0.5], 0)[0]
    projects_df = projects_df.fillna({"project_budget": project_budget_median})
    
    # Select project_id and project_budget from projects_df
    project_budget_df = projects_df.select("project_id", "project_budget")

    # Drop project_budget from logistics_df if it exists
    if 'project_budget' in logistics_df.columns:
        logistics_df = logistics_df.drop('project_budget')

    # Do a left join with logistics_df on project_id
    logistics_df = logistics_df.join(project_budget_df, ['project_id'], 'left')

    # Merging logistics and projects dataframes
    logistics_and_projects_df = logistics_df.join(projects_df, ['project_id', 'project_budget'], 'left')

    # Merging materials and suppliers dataframes
    materials_and_suppliers_df = materials_df.join(suppliers_df, ['supplier_id'], 'left')

    # Shared columns between the 2 resulting datasets
    shared_columns = set(logistics_and_projects_df.columns).intersection(materials_and_suppliers_df.columns)

    # Final merge to create consolidated dataframe
    final_df = logistics_and_projects_df.join(materials_and_suppliers_df, list(shared_columns), 'left')

    # Save the final consolidated dataframe
    final_df.write.mode("overwrite").parquet(os.path.join(data_preprocessed_dir, "raw_data_joined.parquet"))

    # Write the final DataFrame to PostgreSQL
    write_to_db = db_utils.write_to_postgres()
    write_to_db(final_df, "joined_co2_table", "overwrite", jdbc_url, properties)

    return final_df

def main():
    # JDBC URL
    jdbc_url = db_utils.jdbc_url
    db_properties = {
        "user": db_utils.db_username,
        "password": db_utils.db_password,
        "driver": "org.postgresql.Driver"
    }

    # Load DATA_INGESTED_DIR from environment variable
    data_ingested_dir = os.getenv('DATA_INGESTED_DIR')
    data_preprocessed_dir = os.getenv('DATA_PREPROCESSED_DIR')
    if not data_ingested_dir or not data_preprocessed_dir:
        raise ValueError("DATA_INGESTED_DIR or DATA_PREPROCESSED_DIR environment variable not set")

    try:
        transform_data(data_ingested_dir, data_preprocessed_dir, jdbc_url, db_properties)
        logging.info("Data transformation and merging completed successfully.")
    except Exception as e:
        logging.error(f"Error in data transformation: {e}")

if __name__ == "__main__":
    main()