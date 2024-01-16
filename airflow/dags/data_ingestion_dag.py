from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import sys

# Add project root to sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.script_utils import setup_environment
setup_environment(ROOT_DIR)

DATA_INGESTION_FILE = os.getenv('DATA_INGESTION_FILE')

# Dynamically import the module from the given path
spec = importlib.util.spec_from_file_location("data_ingestion_script", DATA_INGESTION_FILE)
data_ingestion_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data_ingestion_script)

# Import the main function from data_ingestion_script
data_ingestion_main = data_ingestion_script.main



