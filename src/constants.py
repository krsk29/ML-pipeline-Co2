import os

#get the project directory path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#data
DATA = os.path.join(BASE_DIR, 'data')

LOGISTICS_DATA = os.path.join(DATA, 'logistics_data.csv')
MATERIALS_DATA = os.path.join(DATA, 'materials_data.csv')
PROJECTS_DATA = os.path.join(DATA, 'projects_data.csv')
SUPPLIERS_DATA = os.path.join(DATA, 'suppliers_data.csv')
FINAL_DATA_parquet = os.path.join(DATA, 'final_df.parquet')
FINAL_DATA_csv = os.path.join(DATA, 'final_df.csv')
