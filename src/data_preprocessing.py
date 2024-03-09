import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, col, year, month, dayofmonth, dayofweek, datediff, to_date, regexp_replace, log
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

from script_utils import configure_logging, read_parquet
from db_utils import DBUtils
from constant_variables import LOGS_DIR, CATEGORICAL_COLUMNS, COLUMNS_TO_SCALE, FINAL_TABLE, DATA_INGESTED_DIR

# Configuring logging to write to a file
configure_logging(LOGS_DIR, 'data_preprocessing')

# Initialise class instance
db_utils = DBUtils()

def convert_date_format(df, col_name):
    """
    Converts date strings in a DF column to date type, handling multiple formats.
    """
    return df.withColumn(
        col_name, 
        when(
            col(col_name).rlike("^\d{4}/\d{2}/\d{2}$"),  # Matches date like 'yyyy/MM/dd'
            to_date(col(col_name), 'yyyy/MM/dd')
        ).otherwise(
            to_date(col(col_name), 'dd/MM/yyyy')  # Assumes the date is in 'dd/MM/yyyy' if not 'yyyy/MM/dd'
        )
    )

def basic_preprocessing(df):
    """
    Applies a series of preprocessing steps to the input DF.
    """
    # Step 1: Replace hyphens with slashes in the transaction_date column
    df = df.withColumn("transaction_date", regexp_replace("transaction_date", "-", "/"))
    
    # Step 2: Convert transaction_date to date type considering different formats
    df = convert_date_format(df, "transaction_date")
    
    # Step 3: Check for nulls in date columns (for demonstration, not altering df)
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in ["transaction_date", "project_start_date", "project_end_date"]]).show()
    
    # Print data types to check the conversion results
    print(df.dtypes)
    
    # Step 4: Extract Year, Month, Day, and other features from transaction_date
    df = df.withColumn("Transaction_Year", year("transaction_date"))
    df = df.withColumn("Transaction_Month", month("transaction_date"))
    df = df.withColumn("Transaction_Day", dayofmonth("transaction_date"))
    df = df.withColumn("is_weekend", (dayofweek("transaction_date").isin([1, 7])).cast("int"))
    df = df.withColumn("project_duration", datediff("project_end_date", "project_start_date"))
    
    # Log transformations
    df = df.withColumn("log_project_budget", log("project_budget"))
    df = df.withColumn("log_CO2_emission", log("CO2_emission"))

    # Create polynomial features for 'Quantity' and 'Distance Covered'
    df = df.withColumn("Quantity_Squared", col("Quantity")**2)
    df = df.withColumn("Distance_Covered_Squared", col("Distance_Covered")**2)

    # Create interaction term between 'Quantity' and 'Distance Covered'
    df = df.withColumn("Quantity_Distance_Interaction", col("Quantity") * col("Distance_Covered"))

    return df

def encoding_and_scaling(df, categorical_columns, columns_to_scale, fit=True, pipeline_model=None):
    """
    Apply encoding, scaling, and assembly to the DataFrame. Fit the pipeline if 'fit' is True.

    :param df: Spark DataFrame to process.
    :param categorical_columns: List of names of the categorical columns.
    :param columns_to_scale: List of names of the columns to scale.
    :param fit: Boolean flag to fit the pipeline or not.
    :return: Transformed DataFrame, and the pipeline model if fitted, otherwise None.
    """
    if fit or not pipeline_model:
        stages = []

        # One-Hot Encoding for categorical columns
        for categoricalCol in categorical_columns:
            stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
            encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "Vec"])
            stages += [stringIndexer, encoder]

        # Assemble numerical columns that need to be scaled
        assembler_for_scaling = VectorAssembler(inputCols=columns_to_scale, outputCol="features_to_scale")
        stages += [assembler_for_scaling]

        # Scale the numerical columns
        scaler = StandardScaler(inputCol="features_to_scale", outputCol="scaledFeatures")
        stages += [scaler]

        # Assemble all features into one vector column
        assembledInputs = [c + "Vec" for c in categorical_columns] + ["scaledFeatures"]
        final_assembler = VectorAssembler(inputCols=assembledInputs, outputCol="features")
        stages += [final_assembler]

        pipeline = Pipeline(stages=stages)
        pipeline_model = pipeline.fit(df)

    transformed_df = pipeline_model.transform(df)
    return pipeline_model, transformed_df

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Data Preprocessing") \
        .getOrCreate()

    engine = db_utils.create_db_engine()

    final_data_generator = db_utils.read_from_postgres(engine, FINAL_TABLE, batch_size=20000)
    db_utils.process_and_save_batches(final_data_generator, os.path.join(DATA_INGESTED_DIR, "FINAL_DATA"), spark)
    df = read_parquet(os.path.join(DATA_INGESTED_DIR, "FINAL_DATA_*.parquet"), spark)

    df_preprocessed = basic_preprocessing(df)
    train_df, test_df = df_preprocessed.randomSplit([0.80, 0.20], seed=42)

    pipeline_model, train_transformed = encoding_and_scaling(train_df, CATEGORICAL_COLUMNS, COLUMNS_TO_SCALE, fit=True)
    _, test_transformed = encoding_and_scaling(test_df, CATEGORICAL_COLUMNS, COLUMNS_TO_SCALE, fit=False, pipeline_model=pipeline_model)

    # Saving the transformed datasets
    train_transformed.write.parquet(DATA_INGESTED_DIR + "/train_transformed.parquet", mode="overwrite")
    test_transformed.write.parquet(DATA_INGESTED_DIR + "/test_transformed.parquet", mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    main()