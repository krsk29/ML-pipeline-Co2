
import os
print("Current working directory:", os.getcwd())
import logging

from pyspark.sql import SparkSession

from pyspark.ml.regression import RandomForestRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from dotenv import load_dotenv
from script_utils import get_env_variable, configure_logging

# from constants import TEST_TRANSFORMED_DATA, TRAIN_TRANSFORMED_DATA, MODELS, RANDOM_FOREST_MODEL
# quickly retrieving data for ml-flow
# Get the directory of the current script
current_script_dir = os.path.dirname(os.path.realpath(__file__))
# Construct the path to the experiments-data directory
data_dir_path = os.path.join(current_script_dir, '..', 'experiments', 'experiments-data')
# Construct the full path to data file
train_data_file_path = os.path.join(data_dir_path, 'train_transformed.parquet')
test_data_file_path = os.path.join(data_dir_path, 'test_transformed.parquet')

# loading env_variables
load_dotenv()
# Configuring logging to write to a file
logs_dir = get_env_variable('LOGS_DIR')
configure_logging(logs_dir, 'modelling')

if __name__ == "__main__":

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("CO2 Emission ML Pipeline - Modelling") \
        .getOrCreate()

    #reading df
    TEST_TRANSFORMED_DF = spark.read.parquet(test_data_file_path)
    TRAIN_TRANSFORMED_DF = spark.read.parquet(train_data_file_path)

    # initialize the Linear Regression model
    lr = LinearRegression(featuresCol="features", labelCol="log_CO2_emission")

    # train model
    lr_model = lr.fit(TRAIN_TRANSFORMED_DF)

    # predict on the test data
    lr_predictions = lr_model.transform(TEST_TRANSFORMED_DF)

    # evaluate the model
    lr_evaluator = RegressionEvaluator(labelCol="log_CO2_emission", predictionCol="prediction", metricName="rmse")
    lr_rmse = lr_evaluator.evaluate(lr_predictions)
    print(f"Root Mean Squared Error (RMSE) on test data = {lr_rmse}")

    lr_evaluator = RegressionEvaluator(labelCol="log_CO2_emission", predictionCol="prediction", metricName="r2")
    lr_r2 = lr_evaluator.evaluate(lr_predictions)
    print(f"R-squared on test data = {lr_r2}")






