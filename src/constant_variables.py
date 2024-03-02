import os

#get the project directory path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#models
# MODELS = os.path.join(BASE_DIR, 'experiments', 'experimentation_models')
# RANDOM_FOREST_MODEL = os.path.join(MODELS, 'random_forest')

#data
DATA = os.path.join(BASE_DIR, 'data')

MATERIALS_DATA = os.path.join(DATA, 'raw', 'materials_0.parquet')
PROJECTS_DATA = os.path.join(DATA, 'raw', 'projects_0.parquet')
SUPPLIERS_DATA = os.path.join(DATA, 'raw', 'suppliers_0.parquet')
LOGISTICS_DATA = os.path.join(DATA, 'raw', 'logistics_0.parquet')
FINAL_DATA_parquet = os.path.join(DATA, 'preprocessed', 'raw_data_joined.parquet')

# pre-processed data for training
TEST_TRANSFORMED_DATA = os.path.join(DATA, 'test_transformed.parquet')
TRAIN_TRANSFORMED_DATA = os.path.join(DATA, 'train_transformed.parquet')

