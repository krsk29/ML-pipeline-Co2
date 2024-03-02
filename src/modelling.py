
import os
import logging

from pyspark.sql import SparkSession

from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

import mlflow
import mlflow.spark

import matplotlib.pyplot as plt
import numpy as np

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

def _plot_residuals(predictions):
    # calculate residuals
    residuals = predictions.select('log_CO2_emission', 'prediction').rdd.map(lambda x: x[0] - x[1]).collect()
    # extract predicted values
    predicted_values = predictions.select('prediction').rdd.map(lambda x: x[0]).collect()
    # plot residuals
    plt.figure(figsize=(5, 3))
    plt.scatter(predicted_values, residuals, alpha=0.2)
    plt.title('Residuals vs Predictions')
    plt.xlabel('Predicted Values')
    plt.ylabel('Residuals')
    plt.axhline(y=0, color='b', linestyle='-')
    fig = plt.gcf()
    plt.close()
    return fig

def _plot_prediction_error_plot(predictions):
    # Assuming 'cv_predictions' is your DataFrame with predictions
    actual = predictions.select('log_CO2_emission').collect()
    predicted = predictions.select('prediction').collect()

    # Convert to a list or a numpy array
    actual_values = [row['log_CO2_emission'] for row in actual]
    predicted_values = [row['prediction'] for row in predicted]

        # Plotting
    plt.scatter(actual_values, predicted_values, alpha=0.5)
    plt.xlabel('Actual Values')
    plt.ylabel('Predicted Values')
    plt.title('Prediction Error Plot')
    plt.plot([min(actual_values), max(actual_values)], [min(actual_values), max(actual_values)], 'k--')  # Diagonal line
    fig = plt.gcf()
    plt.close()
    return fig

def _plot_learning_curves(TEST_TRANSFORMED_DF, TRAIN_TRANSFORMED_DF):
    # Re-initialize the RandomForestRegressor
    rf = RandomForestRegressor(featuresCol="features", labelCol="log_CO2_emission")

    subset_sizes = np.linspace(0.1, 1.0, 15)
    train_scores = []
    test_scores = []

    evaluator = RegressionEvaluator(labelCol="log_CO2_emission", predictionCol="prediction", metricName="rmse")

    for subset_size in subset_sizes:
        # Sample the training data
        subset_train_df = TRAIN_TRANSFORMED_DF.sample(withReplacement=False, fraction=subset_size, seed=33)
        
        # Fit the model on the subset
        model = rf.fit(subset_train_df)
        
        # Evaluate on the subset
        train_predictions = model.transform(subset_train_df)
        train_rmse = evaluator.evaluate(train_predictions)
        train_scores.append(train_rmse)
        
        # Evaluate on the test set
        test_predictions = model.transform(TEST_TRANSFORMED_DF)
        test_rmse = evaluator.evaluate(test_predictions)
        test_scores.append(test_rmse)

    # Plotting
    plt.plot(subset_sizes, train_scores, label='Training Score')
    plt.plot(subset_sizes, test_scores, label='Test Score')
    plt.xlabel('Subset Size')
    plt.ylabel('RMSE')
    plt.title('Learning Curves')
    plt.legend()
    fig = plt.gcf()
    plt.close()
    return fig

def train_linear_regression(TEST_TRANSFORMED_DF, TRAIN_TRANSFORMED_DF):
    # start mlflow run for linear regression
    with mlflow.start_run():
        mlflow.set_tag("model", "Linear Regression")

        # Model training
        lr = LinearRegression(featuresCol="features", labelCol="log_CO2_emission")
        lr_model = lr.fit(TRAIN_TRANSFORMED_DF)
        lr_predictions = lr_model.transform(TEST_TRANSFORMED_DF)

        # Model evaluation
        lr_evaluator = RegressionEvaluator(labelCol="log_CO2_emission", predictionCol="prediction", metricName="rmse")
        lr_rmse = lr_evaluator.evaluate(lr_predictions)
        lr_r2 = lr_evaluator.evaluate(lr_predictions, {lr_evaluator.metricName: "r2"})

        # Generate plots
        lr_residuals_plot_figure = _plot_residuals(lr_predictions) 
        lr_prediction_error_plot = _plot_prediction_error_plot(lr_predictions)
        lr_learning_curves = _plot_learning_curves(TEST_TRANSFORMED_DF, TRAIN_TRANSFORMED_DF)

        # Log params, metrics, model and plots
        mlflow.log_param("model_type", "Linear Regression")
        mlflow.log_metric("rmse", lr_rmse)
        mlflow.log_metric("r2", lr_r2)
        mlflow.spark.log_model(lr_model, "linear_regression_model") 
        mlflow.log_figure(lr_residuals_plot_figure, "residuals_plot_linear_regression.png")
        mlflow.log_figure(lr_prediction_error_plot, "prediction_errors_plot_linear_regression.png")
        mlflow.log_figure(lr_learning_curves, "learning_curves_plot_linear_regression.png")
        
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

        # Generate plots
        gbt_residuals_plot_figure = _plot_residuals(gbt_cv_predictions)
        gbt_prediction_error_plot = _plot_prediction_error_plot(gbt_cv_predictions)
        gbt_learning_curves = _plot_learning_curves(TEST_TRANSFORMED_DF, TRAIN_TRANSFORMED_DF) 

        # Log params, metrics, and model
        mlflow.log_param("model_type", "Gradient Boosted Trees")
        mlflow.log_metric("rmse", gbt_cv_rmse)
        mlflow.log_metric("r2", gbt_cv_r2)
        mlflow.spark.log_model(gbt_cv_model, "gbt_model")
        mlflow.log_figure(gbt_residuals_plot_figure, "residuals_plot_gradient_boosted_trees.png")
        mlflow.log_figure(gbt_prediction_error_plot, "prediction_errors_plot_gradient_boosted_trees.png")
        mlflow.log_figure(gbt_learning_curves, "learning_curves_plot_gradient_boosted_trees.png")

def train_random_forest_regressor(TRAIN_TRANSFORMED_DF, TEST_TRANSFORMED_DF):
    # Start MLflow run for Random Forest Regressom
    with mlflow.start_run():
        mlflow.set_tag("model", "Random Forest Regressom")

        # initialize RandomForest regressor
        rf = RandomForestRegressor(featuresCol="features", labelCol="log_CO2_emission")

        # parameter grid 
        paramGrid = (ParamGridBuilder()
                    .addGrid(rf.numTrees, [5, 20])  # List of trees to test
                    .addGrid(rf.maxDepth, [3, 5])    # List of maximum depths to test
                    .addGrid(rf.maxBins, [24])        # List of bins to test
                    .build())

        # evaluator for the cross-validation
        rf_evaluator = RegressionEvaluator(labelCol="log_CO2_emission", predictionCol="prediction", metricName="rmse")

        # crossValidator requires the same evaluator used to evaluate the model
        cv = CrossValidator(estimator=rf,
                            estimatorParamMaps=paramGrid,
                            evaluator=rf_evaluator,
                            numFolds=3)  # Number of folds for cross-validation

        # run cross-validation
        cv_model = cv.fit(TRAIN_TRANSFORMED_DF)

        # Use the best model found to make predictions on the test data
        cv_predictions = cv_model.transform(TEST_TRANSFORMED_DF)

        # evaluate best model
        cv_rmse = rf_evaluator.evaluate(cv_predictions)

        #R-squared eevaluation
        cv_r2 = rf_evaluator.evaluate(cv_predictions, {rf_evaluator.metricName: "r2"})

        # Get best model
        best_rf_model = cv_model.bestModel

        # Generate plots
        rf_residuals_plot_figure = _plot_residuals(cv_predictions)
        rf_prediction_error_plot = _plot_prediction_error_plot(cv_predictions)
        rf_learning_curves = _plot_learning_curves(TEST_TRANSFORMED_DF, TRAIN_TRANSFORMED_DF) 

        # Log params, metrics, and model
        mlflow.log_param("model_type", "Random Forest")
        mlflow.log_metric("rmse", cv_rmse)
        mlflow.log_metric("r2", cv_r2)
        mlflow.spark.log_model(best_rf_model, "random_forest_model")
        mlflow.log_figure(rf_residuals_plot_figure, "residuals_plot_random_forest.png")
        mlflow.log_figure(rf_prediction_error_plot, "prediction_errors_plot_random_forest.png")
        mlflow.log_figure(rf_learning_curves, "learning_curves_plot_random_forest.png")

def main():
    mlflow.set_tracking_uri("http://127.0.0.1:5000")
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

    # Train and log Random Forest model
    train_random_forest_regressor(TRAIN_TRANSFORMED_DF, TEST_TRANSFORMED_DF)
    
if __name__ == "__main__":
    main()