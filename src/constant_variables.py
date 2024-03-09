import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

LOGS_DIR = os.getenv('LOGS_DIR')
DB_URL = os.getenv('DB_URL')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DATA_DIR = os.getenv('DATA_DIR')
DB_HOST =  os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DATA_INGESTED_DIR = os.getenv('DATA_INGESTED_DIR')
DATA_PREPROCESSED_DIR = os.getenv('DATA_PREPROCESSED_DIR')
JDBC_DRIVER = os.getenv('JDBC_DRIVER')
DATA_INGESTION_FILE = os.getenv('DATA_INGESTION_FILE')

# db tables
LOGISTICS_TABLE = "logistics"
MATERIALS_TABLE = "materials"
PROJECTS_TABLE = "projects"
SUPPLIERS_TABLE = "suppliers"
FINAL_TABLE = "joined_co2_table"
PREPROCESSED_TABLE = ""
TRAINING_TABLE = ""
TESTING_TABEL = "" 

# get the project directory path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# data
DATA = os.path.join(BASE_DIR, 'data')

MATERIALS_DATA = os.path.join(DATA, 'raw', 'materials_0.parquet')
PROJECTS_DATA = os.path.join(DATA, 'raw', 'projects_0.parquet')
SUPPLIERS_DATA = os.path.join(DATA, 'raw', 'suppliers_0.parquet')
LOGISTICS_DATA = os.path.join(DATA, 'raw', 'logistics_0.parquet')
FINAL_DATA_parquet = os.path.join(DATA, 'preprocessed', 'raw_data_joined.parquet')

# pre-processed data for training
TEST_TRANSFORMED_DATA = os.path.join(DATA, 'test_transformed.parquet')
TRAIN_TRANSFORMED_DATA = os.path.join(DATA, 'train_transformed.parquet')

# categorical columns
CATEGORICAL_COLUMNS = ["transport_mode", "project_location", "material_category", "supplier_location"]
# columns to scale
COLUMNS_TO_SCALE = ["Quantity_Squared", "Distance_Covered_Squared", "Quantity_Distance_Interaction", "supplier_rating", "Transaction_Year", "Transaction_Month", "project_duration"]
# date columns
DATE_COLUMNS = ['transaction_date', 'project_start_date', 'project_end_date']
