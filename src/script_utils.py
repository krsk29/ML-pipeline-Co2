import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv


def get_env_variable(var_name, default=None):
    """
    Safely get an environment variable. If the variable is not found, it raises a ValueError.
    """
    value = os.getenv(var_name, default)
    if value is None:
        raise ValueError(f"{var_name} environment variable not set")
    return value

def configure_logging(logs_dir, log_prefix):
    """
    Configures logging to write to a file with the given directory and log prefix.
    """
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f'{logs_dir}/{log_prefix}_{current_time}.log'
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_environment(root_dir):
    """
    Sets up the environment by loading the .env file and configuring the Python path.
    """
    # Load .env file
    env_file = os.path.join(root_dir, '.env')
    load_dotenv(env_file)

    # Add src directory to Python path
    src_dir = os.path.join(root_dir, 'src')
    if src_dir not in sys.path:
        sys.path.append(src_dir)

# Function to read Parquet files into DataFrame
def read_parquet(file_pattern, spark):
    """
    Reads Parquet files matching a pattern into a DataFrame.
    """
    return spark.read.parquet(file_pattern)



