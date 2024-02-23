
import os
import logging

from pyspark.sql import SparkSession

from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

import mlflow
import mlflow.spark

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

def train_linear_regression(TEST_TRANSFORMED_DF, TRAIN_TRANSFORMED_DF):
    # start mlflow run for linear regression
    with mlflow.start_run():
        mlflow.set_tag("model", "Linear Regression")

        # Model training and evaluation
        lr = LinearRegression(featuresCol="features", labelCol="log_CO2_emission")
        lr_model = lr.fit(TRAIN_TRANSFORMED_DF)
        lr_predictions = lr_model.transform(TEST_TRANSFORMED_DF)

        lr_evaluator = RegressionEvaluator(labelCol="log_CO2_emission", predictionCol="prediction", metricName="rmse")
        lr_rmse = lr_evaluator.evaluate(lr_predictions)
        lr_r2 = lr_evaluator.evaluate(lr_predictions, {lr_evaluator.metricName: "r2"})

        # Log params, metrics, and model
        mlflow.log_param("model_type", "Linear Regression")
        mlflow.log_metric("rmse", lr_rmse)
        mlflow.log_metric("r2", lr_r2)
        mlflow.spark.log_model(lr_model, "linear_regression_model") 

def train_gradient_boosted_trees(TRAIN_TRANSFORMED_DF, TEST_TRANSFORMED_DF):
    # Start MLflow run for Gradient Boosted Trees
    with mlflow.start_run():
        mlflow.set_tag("model", "Gradient Boosted Trees")

        # Model training and evaluation
        gbt = GBTRegressor(featuresCol="features", labelCol="log_CO2_emission")
        gbt_paramGrid = (ParamGridBuilder()
                         .addGrid(gbt.maxDepth, [2, 3])
                         .addGrid(gbt.maxBins, [3])
                         .addGrid(gbt.maxIter, [4])
                         .build())

        gbt_evaluator = RegressionEvaluator(labelCol="log_CO2_emission", predictionCol="prediction", metricName="rmse")
        gbt_cv = CrossValidator(estimator=gbt,
                                estimatorParamMaps=gbt_paramGrid,
                                evaluator=gbt_evaluator,
                                numFolds=3)

        gbt_cv_model = gbt_cv.fit(TRAIN_TRANSFORMED_DF)
        gbt_cv_predictions = gbt_cv_model.transform(TEST_TRANSFORMED_DF)

        gbt_cv_rmse = gbt_evaluator.evaluate(gbt_cv_predictions)
        gbt_cv_r2 = gbt_evaluator.evaluate(gbt_cv_predictions, {gbt_evaluator.metricName: "r2"})

        # Log params, metrics, and model
        mlflow.log_param("model_type", "Gradient Boosted Trees")
        mlflow.log_metric("rmse", gbt_cv_rmse)
        mlflow.log_metric("r2", gbt_cv_r2)
        mlflow.spark.log_model(gbt_cv_model, "gbt_model")

def main():
    # MLflow experiment setup
    mlflow.set_experiment(experiment_name="CO2 Emission Models")

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("CO2 Emission ML Pipeline - Modelling") \
        .getOrCreate()

    # Data preparation
    TEST_TRANSFORMED_DF = spark.read.parquet(test_data_file_path)
    TRAIN_TRANSFORMED_DF = spark.read.parquet(train_data_file_path)

    # Train and log Linear Regression model
    train_linear_regression(TRAIN_TRANSFORMED_DF, TEST_TRANSFORMED_DF)

    # Train and log Gradient Boosted Trees model
    train_gradient_boosted_trees(TRAIN_TRANSFORMED_DF, TEST_TRANSFORMED_DF)

    
if __name__ == "__main__":
    main()