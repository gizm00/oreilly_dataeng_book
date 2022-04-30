from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession
from pathlib import Path
import logging 
import re

# Monkey patching the DataFrame transform method for Spark 2.4
# This is available by default in Spark 3.0
def transform(self, f):
    return f(self)
DataFrame.transform = transform

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Using matched groups, we can extract information from the taxi file names
TAXI_DATA_PATTERN = "(?P<service>[a-zA-Z0-9]+)_tripdata_(?P<year>[0-9]{4})-(?P<month>[0-9]{2}).csv"
MY_BUCKET_NAME = 'data-scale-oreilly-sev'
TAXI_DATA_DEST = f"s3://{MY_BUCKET_NAME}/data/emr/nyc-taxi/taxi-data/output/section2/json"
TAXI_LOOKUP_DEST = f"s3://{MY_BUCKET_NAME}/data/emr/nyc-taxi/zone-lookup/output/section2/json"
spark = None

def transform_yellow_taxi(df):
    return (df.withColumn("pickup_datetime", f.col("tpep_pickup_datetime").cast(t.TimestampType()))
        .withColumn("dropoff_datetime", f.col("tpep_dropoff_datetime").cast(t.TimestampType()))
        .withColumn("passenger_count", f.col("passenger_count").cast(t.IntegerType()))
        .withColumn("trip_distance", f.col("trip_distance").cast(t.FloatType()))
        .withColumn("fare_amount", f.col("fare_amount").cast(t.FloatType()))
        .withColumn("tip_amount", f.col("tip_amount").cast(t.FloatType()))
        .withColumn("PULocationID", f.col("PULocationID").cast(t.IntegerType()))
        .withColumn("DOLocationID", f.col("DOLocationID").cast(t.IntegerType())))
        
        
def transform_green_taxi(df):
    return (df.withColumn("pickup_datetime", f.col("lpep_pickup_datetime").cast(t.TimestampType()))
        .withColumn("dropoff_datetime", f.col("lpep_dropoff_datetime").cast(t.TimestampType()))
        .withColumn("passenger_count", f.col("passenger_count").cast(t.IntegerType()))
        .withColumn("trip_distance", f.col("trip_distance").cast(t.FloatType()))
        .withColumn("fare_amount", f.col("fare_amount").cast(t.FloatType()))
        .withColumn("tip_amount", f.col("tip_amount").cast(t.FloatType()))
        .withColumn("PULocationID", f.col("PULocationID").cast(t.IntegerType()))
        .withColumn("DOLocationID", f.col("DOLocationID").cast(t.IntegerType())))
 

def transform_fhv(df):
    return (df.withColumn("pickup_datetime", f.col("pickup_datetime").cast(t.TimestampType()))
        .withColumn("dropoff_datetime", f.col("dropoff_datetime").cast(t.TimestampType()))
        .withColumn("PULocationID", f.col("PULocationID").cast(t.IntegerType()))
        .withColumn("DOLocationID", f.col("DOLocationID").cast(t.IntegerType())))

def extract_file_info(file_name):
    m = re.match(TAXI_DATA_PATTERN, file_name)
    if m is not None:
        return (m.group(1), m.group(2), m.group(3))
    
def ingest_taxi_data_multi_service(file_name):
    print(f"Processing {file_name}")
    (service, year, month) = extract_file_info(Path(file_name).name)
    input_df = spark.read.option('header', True).csv(file_name)
    
    if service == 'yellow':
        df_transform = transform_yellow_taxi(input_df)
    elif service == 'green':
        df_transform = transform_green_taxi(input_df)
    else:
        # FHV. What happens if there are more taxi services added?
        df_transform = transform_fhv(input_df)

    (df_transform
         .withColumn("service", f.lit(service))
         .withColumn("year", f.lit(year))
         .withColumn("month", f.lit(month))
         .write
         .mode("append")
         .json(TAXI_DATA_DEST)
    )

def taxi_zone_transform(df):
    return (df.withColumn("LocationID", f.col("LocationID").cast(t.IntegerType()))
            .withColumn("Borough", f.col("Borough").cast(t.StringType()))
            .withColumn("Zone", f.col("Zone").cast(t.StringType()))
            .withColumn("service_zone", f.col("service_zone").cast(t.StringType())))

def ingest_taxi_lookup():
    (spark.read
    .option("header", True)
    .csv("s3://nyc-tlc/misc/taxi _zone_lookup.csv")
    .transform(taxi_zone_transform)
    .write
    .mode("overwrite")
    .json(TAXI_LOOKUP_DEST))
    

if __name__ == '__main__':
    taxi_data_prefix = "s3://nyc-tlc/trip data/"
    taxi_data_files = [
        "yellow_tripdata_2020-01.csv",
        "yellow_tripdata_2020-02.csv",
        "yellow_tripdata_2020-03.csv",
        "yellow_tripdata_2020-04.csv",
        "yellow_tripdata_2020-05.csv",
        "yellow_tripdata_2020-06.csv", 
        "yellow_tripdata_2020-07.csv",
        "yellow_tripdata_2020-08.csv",
        "yellow_tripdata_2020-09.csv",
        "yellow_tripdata_2020-10.csv",
        "yellow_tripdata_2020-11.csv",
        "yellow_tripdata_2020-12.csv",
        "green_tripdata_2020-01.csv", 
        "green_tripdata_2020-02.csv", 
        "green_tripdata_2020-03.csv", 
        "green_tripdata_2020-04.csv", 
        "green_tripdata_2020-05.csv", 
        "green_tripdata_2020-06.csv", 
        "green_tripdata_2020-07.csv", 
        "green_tripdata_2020-08.csv", 
        "green_tripdata_2020-09.csv", 
        "green_tripdata_2020-10.csv", 
        "green_tripdata_2020-11.csv", 
        "green_tripdata_2020-12.csv", 
        "fhv_tripdata_2020-01.csv", 
        "fhvhv_tripdata_2020-01.csv"]
    spark = SparkSession.builder.appName("IngestJob").getOrCreate()
    for file_name in taxi_data_files: 
        taxi_data_path = f"{taxi_data_prefix}{file_name}"
        try:
            ingest_taxi_data_multi_service(taxi_data_path)
        except BaseException:
            logger.exception("Could not process file %s", taxi_data_path)
    ingest_taxi_lookup()