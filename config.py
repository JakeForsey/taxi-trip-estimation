import os

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType, DateType

# dev | prod
MODE = 'dev'

DATA_DIR = f'data/{MODE}'
PLOT_DIR = f'output/plots/{MODE}'

TAXI_DATA_URLS = [
    # 20016 & 2017 green cab
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-01.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-02.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-03.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-04.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-05.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-06.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-07.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-08.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-09.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-10.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-11.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-12.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-01.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-02.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-03.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-04.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-05.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-06.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-07.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-08.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-09.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-10.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-11.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2015-12.csv',
    # 20016 & 2017 yellow cab
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-02.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-03.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-04.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-05.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-06.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-07.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-08.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-09.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-10.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-11.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-12.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-01.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-02.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-03.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-04.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-05.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-06.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-07.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-08.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-09.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-10.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-11.csv',
    'http://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-12.csv',
]
TAXI_DATA_DIR = f'{DATA_DIR}/taxi_data'
TAXI_DATA_SCHEMA = StructType([
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True)
])
TAXI_ID_COL = 'trip_id'
TAXI_VOLUME_PLOT_FILE = f'{PLOT_DIR}/taxi-volumes.png'

ACCIDENT_DATA_URLS = [
    'https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv'
]
ACCIDENT_DATA_DIR = f'{DATA_DIR}/accident_data'
ACCIDENT_DATA_SCHEMA = StructType([
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("number_of_persons_injured", IntegerType(), True),
    StructField("number_of_persons_killed", IntegerType(), True),
])
ACCIDENT_ID_COL = 'accident_id'
ACCIDENT_VOLUME_PLOT_FILE = f'{PLOT_DIR}/accident-volumes.png'

# How many attempts to make at downloading data before skipping
MAX_CSV_DOWNLOAD_RETRIES = 5

# How many accidents to pivot / create features for
ACCIDENT_COUNT = 5

# Export some key environment variables (make sure this points to the python used to execute the app)
os.environ["PYSPARK_PYTHON"] = "/home/jake/.conda/envs/pyspark-env/bin/python"

# Create directories
if not os.path.isdir(DATA_DIR):
    os.makedirs(DATA_DIR)

if not os.path.isdir(PLOT_DIR):
    os.makedirs(PLOT_DIR)

if not os.path.isdir(TAXI_DATA_DIR):
    os.makedirs(TAXI_DATA_DIR)

if not os.path.isdir(ACCIDENT_DATA_DIR):
    os.makedirs(ACCIDENT_DATA_DIR)
