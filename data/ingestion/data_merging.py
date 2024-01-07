from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os
from dotenv import load_dotenv

# Initialize Spark session
spark = SparkSession.builder.appName("Data Merging").getOrCreate()

# Function to read Parquet files into DataFrame
def read_parquet(file_pattern):
    return spark.read.parquet(file_pattern)

# Function to perform data transformations
def transform_data(data_ingested_dir):
    # Read ingested data
    logistics_df = read_parquet(os.path.join(data_ingested_dir, "logistics_*.parquet"))
    materials_df = read_parquet(os.path.join(data_ingested_dir, "materials_*.parquet"))
    projects_df = read_parquet(os.path.join(data_ingested_dir, "projects_*.parquet"))
    suppliers_df = read_parquet(os.path.join(data_ingested_dir, "suppliers_*.parquet"))

    # Handling null values and dropping columns
    # For 'distance_covered' and 'CO2_emission' in logistics data
    distance_covered_median = logistics_df.approxQuantile("distance_covered", [0.5], 0)[0]
    CO2_median = logistics_df.approxQuantile("CO2_emission", [0.5], 0)[0]
    logistics_df = logistics_df.fillna({"distance_covered": distance_covered_median, "CO2_emission": CO2_median})
    logistics_df = logistics_df.drop("supplier_rating")

    # For 'project_budget' in projects data
    project_budget_median = projects_df.approxQuantile("project_budget", [0.5], 0)[0]
    projects_df = projects_df.fillna({"project_budget": project_budget_median})

    # Merging logistics and projects dataframes
    logistics_and_projects_df = logistics_df.join(projects_df, ['project_id', 'project_budget'], 'left')

    # Merging materials and suppliers dataframes
    materials_and_suppliers_df = materials_df.join(suppliers_df, ['supplier_id'], 'left')

    # Shared columns between the 2 resulting datasets
    shared_columns = set(logistics_and_projects_df.columns).intersection(materials_and_suppliers_df.columns)

    # Final merge to create consolidated dataframe
    final_df = logistics_and_projects_df.join(materials_and_suppliers_df, list(shared_columns), 'left')

    # Save the final consolidated dataframe
    final_df.write.parquet(os.path.join(data_ingested_dir, "final_consolidated.parquet"))

    return final_df

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    # Load DATA_INGESTED_DIR from environment variable
    data_ingested_dir = os.getenv('DATA_INGESTED_DIR')
    if not data_ingested_dir:
        raise ValueError("DATA_INGESTED_DIR environment variable not set")

    transform_data(data_ingested_dir)
