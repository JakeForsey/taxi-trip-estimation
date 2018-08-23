import logging
import math
import os
from urllib.error import HTTPError

import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
# Some functions imports don't play ball with PyCharm (rank, col sum) but they do work
# TODO input functions as F and refactor
from pyspark.sql.functions import date_format, window, countDistinct, unix_timestamp, udf, struct, rank, col, sum, \
    lit, concat, first, monotonically_increasing_id, datediff
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType

from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

import pandas as pd

import config


def download_data(urls, data_dir):
    """
    Downloads data from a list of urls and saves them locally

    :param urls: list of urls from which to download csvs
    :param data_dir: directory into which to save the downloaded csvs
    :return: None
    """
    for url in urls:
        file_name = url.split("/")[-1]
        retry_count = 0
        while file_name not in os.listdir(data_dir) and retry_count < config.MAX_CSV_DOWNLOAD_RETRIES:
            try:
                logging.info(f'Downloading {file_name}')
                df = pd.read_csv(url)

                logging.info(f'Saving {file_name}')
                df.to_csv(f'{data_dir}/{file_name}')

            # if the url was broken then skip
            except HTTPError as e:
                logging.warning(f'{url} was not a valid url, skipping')

            # if the connection temporarily failed then retry
            except ConnectionResetError as e:
                logging.warning(
                    f'Connection timed out whilst downloading {url}, retrying (attempt {retry_count + 1}/{config.MAX_CSV_DOWNLOAD_RETRIES})')

                retry_count += 1


def load_data(spark_session, data_dir, schema):
    """
    Loads data into a spark dataframe using the provided spark session, data directory and schema

    :param spark_session: SparkSession
    :param data_dir: directory containing .csv files
    :param schema: spark StructType
    :return: PySpark DataFrame
    """
    logging.info(f'Loading data from {data_dir}')

    df = spark_session.read.csv(
        f'{data_dir}/*.csv',
        header=True,
        inferSchema=True
    )

    # Reformat column names
    # DATE -> date
    # NUMBER OF PERSONS KILLED -> number_of_persons_killed
    # tpep_datetime -> datetime
    for column in df.schema.names:
        df = df.withColumnRenamed(column, column.lower().replace('tpep_', '').replace('lpep_', '').replace(' ', '_'))

    # create a new dataframe containing just the columns of interest
    df = df.select(schema.fieldNames())

    logging.info(f'Records loaded: {df.count()}')

    # df.schema isnt a schema with the treeString() function so need to use this private property
    logging.info(f'Data schema: \n{df._jdf.schema().treeString()}')

    return df


def timestamps_to_features(data, timestamp_col):
    logging.info(f'Creating features from {timestamp_col}')

    # Create new machine learning features from the timestamp
    data = data.withColumn(f'{timestamp_col}.day_of_week', date_format(timestamp_col, 'u').cast('integer'))
    data = data.withColumn(f'{timestamp_col}.hour_of_day', date_format(timestamp_col, 'H').cast('integer'))
    data = data.withColumn(f'{timestamp_col}.month_of_year', date_format(timestamp_col, 'M').cast('integer'))

    return data


def plot_summary(data, timestamp_col, distinct_col, plot_file):

    # If the plot does not already exist then make and save it
    if not os.path.isfile(plot_file):
        logging.info(f'Grouping data using "{timestamp_col}"')
        group = data.groupBy(
            window(timestamp_col, "1 day")
        ).agg(
            countDistinct(distinct_col).alias('count_distinct')
        )
        group = group.select([group.window.start.alias('window_start'), 'count_distinct'])

        logging.info('Exporting Spark dataframe to Pandas dataframe')
        pd_data = group.toPandas().set_index("window_start")

        logging.info('Plotting volume over time')
        fig, axis = plt.subplots(1, 1, figsize=(12, 8))
        pd_data[['count_distinct']].plot(ax=axis)
        axis.set_ylabel('Volume')
        axis.set_xlabel('Date')
        plt.savefig(plot_file)


def haversine_distance(row):
    """
    Function for use as a PySpark udf (user defined function).

    Calculates the spherical distance between the two coordinates passed in.
    :param row: column (Struct) that contains the following 4 values:
                    latitude of point 1, longitude of point 1, latitude of point 2, latitude of point 2
    :return: FloatType distance in miles between the two points
    """
    lat1, lon1, lat2, lon2 = row

    # if any of the values (coordinates) in row are None then return a very large distance
    # TODO also check for inappropriate data types (e.g. str)
    if None in [lat1, lon1, lat2, lon2]:
        return 999999

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # 3959 is the radius of earth in miles
    return 3959 * c


def pivot_column(df, column, distinct_col):
    """
    create machine learning features by pivotting that data

    :param df: Dataframe with multiple records per sample and a rank column
    :param column: column name to pivot on
    :param distinct_col: column name with a unique value to use for grouping (1 value per trip)
    :return: pivoted dataframe containing the new features to be joined onto the dataframe
    """
    # 1. create a column populated with the name that will be the pivoted columns
    # 2. group by something unique to the trip
    # 3. pivot on the rank
    # 4. grab the first record (there will only be one thanks to the rank function
    return df.withColumn(f'{column}_rank', concat(lit(f'{column}_'), col('rank')))\
        .groupBy(distinct_col)\
        .pivot(f'{column}_rank')\
        .agg(first(column))


def merge_accidents(taxi_data, accident_data):
    if config.MODE == 'dev':
        logging.info(f'Selecting a random 0.1% of data before merging, this is to save time in dev mode')
        # Get a random 1% of data with random seed=1
        splits = taxi_data.randomSplit([0.999, 0.001], 1)
        taxi_data = splits[1]

    conditions = [
        # Join all accidents that on are the same date as the taxi trip
        taxi_data['pickup_datetime'].cast('date') == accident_data['accident_timestamp'].cast('date'),
        # Exclude accidents that have null coordinates as they will be useless to use
        accident_data['latitude'].isNotNull(),
        accident_data['longitude'].isNotNull()
    ]
    # This join results in a large number of rows per trip
    temp_df = taxi_data.join(accident_data, conditions, 'left_outer')

    # Calculate the distance between the pickup location and the accident
    distance_udf = udf(haversine_distance, FloatType())
    temp_df = temp_df.withColumn('pickup_to_accident', distance_udf(struct('pickup_latitude', 'pickup_longitude', 'latitude', 'longitude')))

    # --- Create the machine learning features based on the accidents
    # Partitioned by taxi trip, sort by distance from pickup to accident
    partition = Window.partitionBy(temp_df[config.TAXI_ID_COL]).orderBy(temp_df['pickup_to_accident'].asc())
    # Select the first config.ACCIDENT_COUNT rows for each trip
    temp_df = temp_df.select('*', rank().over(partition).alias('rank')).filter(col('rank') <= config.ACCIDENT_COUNT)

    proximity_feature = pivot_column(temp_df, 'pickup_to_accident', config.TAXI_ID_COL)
    taxi_data = taxi_data.join(proximity_feature, [config.TAXI_ID_COL], 'left_outer')

    killed_feature = pivot_column(temp_df, 'number_of_persons_killed', config.TAXI_ID_COL)
    taxi_data = taxi_data.join(killed_feature, [config.TAXI_ID_COL], 'left_outer')

    injured_feature = pivot_column(temp_df, 'number_of_persons_injured', config.TAXI_ID_COL)
    taxi_data = taxi_data.join(injured_feature, [config.TAXI_ID_COL], 'left_outer')

    return taxi_data


def main():
    """
     - Downloads outstanding data
     - Sets up Spark environment
     - Loads data
     - Summarises data
     - Merges data
     - Prepares data for modelling
    :return: None
    """
    # --- Download data (if its not already downloaded)
    if config.MODE == 'prod':
        # In production mode we want to download all the csv files
        # Datasets in develop are controlled by the user
        # Download and save taxi journey data
        download_data(config.TAXI_DATA_URLS, config.TAXI_DATA_DIR)
        # Download and save road traffic accident data
        download_data(config.ACCIDENT_DATA_URLS, config.ACCIDENT_DATA_DIR)

    # --- Set up Spark environment
    spark = SparkSession.builder.appName('Basics').getOrCreate()

    # --- Load data
    # Load and parse taxi data, add an id column
    taxi_df = load_data(spark,
                        data_dir=config.TAXI_DATA_DIR,
                        schema=config.TAXI_DATA_SCHEMA)\
        .withColumn(config.TAXI_ID_COL, monotonically_increasing_id())

    # Load and parse accident data, add an id column and a timestamp column
    accident_df = load_data(spark,
                            data_dir=config.ACCIDENT_DATA_DIR,
                            schema=config.ACCIDENT_DATA_SCHEMA)\
        .withColumn(config.ACCIDENT_ID_COL, monotonically_increasing_id())\
        .withColumn('accident_timestamp', unix_timestamp('date', 'MM/dd/yyyy').cast('timestamp'))

    # --- Summarise data
    # Plot and save data summary (if its not already saved)
    plot_summary(taxi_df, 'pickup_datetime', config.TAXI_ID_COL, config.TAXI_VOLUME_PLOT_FILE)
    plot_summary(accident_df, 'accident_timestamp', config.ACCIDENT_ID_COL, config.ACCIDENT_VOLUME_PLOT_FILE)

    # --- Create ML features
    # Merge nearby accidents with taxi trips (this is a very long running process)
    df = merge_accidents(taxi_df, accident_df)

    # Create day of week, hour of day values from time stamp
    df = timestamps_to_features(df, 'pickup_datetime')
    df = timestamps_to_features(df, 'dropoff_datetime')

    df = df.drop('pickup_datetime')
    df = df.drop('dropoff_datetime')

    # --- Log the results of the data preparation stages
    logging.info('Data preparation complete')
    logging.info(f'Number of rows: {df.count()}')
    logging.info(f'Merged data: \n{df.show(100)}')
    logging.info(f'Data schema: \n{df._jdf.schema().treeString()}')

    # --- Train a gradient boosted trees model to predict the duration of a trip

    # Create the target variable (label is a special column name in MLlib
    df = df.withColumn('label', datediff('pickup_datetime', 'dropoff_datetime'))
    print(df.show(100))
    ignore = ['label']
    assembler = VectorAssembler(
        inputCols=[x for x in df.columns if x not in ignore],
        outputCol='features')
    assembler.transform(df)

    feature_indexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures").fit(df)

    # Split the data into training and test sets (30% held out for testing)
    (training_df, testing_df) = df.randomSplit([0.7, 0.3])

    # Train a GBT model.
    gbt = GBTRegressor(featuresCol="indexedFeatures", maxIter=10)

    # Chain indexer and GBT in a Pipeline
    pipeline = Pipeline(stages=[assembler, feature_indexer, gbt])

    # Train model.  This also runs the indexer.
    model_pipeline = pipeline.fit(training_df)

    # Make predictions.
    predictions = model_pipeline.transform(testing_df)

    # Select example rows to display
    predictions.select("prediction", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    logging.info(f'Root Mean Squared Error (RMSE) on test data: {rmse}')

    gbt_model = model_pipeline.stages[1]
    logging.info(gbt_model)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
