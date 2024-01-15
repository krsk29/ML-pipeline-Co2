from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import sys

from src.script_utils import setup_environment

# Setup environment 
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
setup_environment(ROOT_DIR)

DATA_INGESTION_FILE = os.getenv('DATA_INGESTION_FILE')

# Dynamically import the module from the given path
spec = importlib.util.spec_from_file_location("data_ingestion_script", DATA_INGESTION_FILE)
data_ingestion_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data_ingestion_script)

# Import the main function from data_ingestion_script
data_ingestion_main = data_ingestion_script.main


