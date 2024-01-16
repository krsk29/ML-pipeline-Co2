from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import sys

# Setup environment 
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
SRC_DIR = os.path.join(ROOT_DIR, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

print(sys.path)

from src.script_utils import setup_environment
setup_environment(ROOT_DIR)

DATA_INGESTION_FILE = os.getenv('DATA_INGESTION_FILE')

# Dynamically import the module from the given path
spec = importlib.util.spec_from_file_location("data_ingestion_script", DATA_INGESTION_FILE)
data_ingestion_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data_ingestion_script)

# Import the main function from data_ingestion_script
data_ingestion_main = data_ingestion_script.main

print(data_ingestion_main)


