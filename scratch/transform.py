from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f, types as t, Window
from pyspark.sql import SparkSession
import requests
from datetime import timedelta, datetime

MY_BUCKET_NAME = 'data-scale-oreilly-sev'
TAXI_DATA_SRC = f"s3://{MY_BUCKET_NAME}/data/emr/nyc-taxi/taxi-data/output/section2/json"
TAXI_LOOKUP_SRC = f"s3://{MY_BUCKET_NAME}/data/emr/nyc-taxi/zone-lookup/output/section2/json"
spark = None

def get_taxi_df():
    taxiDF = spark.read.json(TAXI_DATA_SRC)
    taxi_lookup = spark.read.json(TAXI_LOOKUP_SRC)
    taxi_filtered = (taxiDF
     .filter(taxiDF.pickup_datetime.isNotNull())
     .filter(taxiDF.dropoff_datetime.isNotNull()))
                     
    groupDF = taxi_filtered.join(taxi_lookup.select("Borough", "LocationID"), taxi_filtered.PULocationID == taxi_lookup.LocationID)
    groupDF.show()
    return groupDF

def get_zip_code_mapping_df():
    zipReadDF = spark.read.option('header', True).csv(f"s3://{MY_BUCKET_NAME}/data/borough-zip-mapping/ny-zip-codes.csv")
    returnZipDF = zipReadDF.select('Borough', 'Neighborhood', f.explode(f.split('ZIP Codes', ',')).alias('zip'))
    returnZipDF.show()
    return returnZipDF


def get_air_quality_df(zipDF):
#     zipList = [x[0] for x in zipDF.select('zip').collect()]
    zipList = ['11212', '10023']
    airQualitySchema = t.StructType([
        t.StructField("AQI", t.LongType(), True),
        t.StructField("Category", t.MapType(t.StringType(), t.LongType()), True),
        t.StructField("DateObserved", t.StringType(), True),
        t.StructField("HourObserved", t.LongType(), True),
        t.StructField("Latitude", t.DoubleType(), True),
        t.StructField("LocalTimeZone", t.StringType(), True),
        t.StructField("Longitude", t.DoubleType(), True),
        t.StructField("ParameterName", t.StringType(), True),
        t.StructField("ReportingArea", t.StringType(), True),
        t.StructField("StateCode", t.StringType(), True),
        t.StructField("zip", t.StringType(), True),
        ])
    
    airDF = spark.createDataFrame([], airQualitySchema)

    for zipCode in zipList:
        for d in range(0,99):
            startDate = datetime.strptime("2020-06-01", "%Y-%m-%d").date()
            dateDelta = timedelta(days=d)
            endDate = startDate + dateDelta
            
            apiPath = f"https://www.airnowapi.org/aq/observation/zipCode/historical/?format=application/json&zipCode={zipCode}&date={endDate}T00-0000&distance=100&API_KEY=B79C872D-FC7A-4037-BFA6-F064276F2EB3"
            request = requests.get(apiPath)
            requestDF = spark.createDataFrame(request.json())
            requestDF = requestDF.withColumn('zip', f.lit(zipCode))
            airDF = airDF.unionAll(requestDF)
        
    returnAirDF = airDF.withColumn("categoryNumber", f.col("Category.Number"))
    returnAirDF.show()
    
    return returnAirDF

def calculate_hottest_days(taxiDF, airQualityDF, zipDF):
    joinAirDF = airQualityDF.filter("zip IN ('11212', '10023')").withColumn('air_day', f.date_format("dateObserved", "yyyyMMdd")).withColumnRenamed("zip", "air_zip")
    
    zipDF = zipDF.withColumnRenamed('Borough', 'zipBorough')
    
    taxiZipDF = taxiDF.join(zipDF.select('zip', 'ZipBorough'), zipDF.zipBorough == taxiDF.Borough)
    
    joinTaxiDF = (taxiZipDF.withColumn("pickup_day", f.date_format("pickup_datetime", "yyyyMMdd"))
                        .withColumn("pickup_month", f.date_format("pickup_datetime", "yyyyMM")))

    joinCondition = [joinTaxiDF.pickup_day == joinAirDF.air_day, joinTaxiDF.zip == joinAirDF.air_zip]
    
    joinedAirDF = joinTaxiDF.join(joinAirDF.select('categoryNumber', 'air_day', 'air_zip'), joinCondition)
    
    aggDF = (joinedAirDF.groupBy('pickup_month', 'pickup_day', 'zip')
                        .agg(f.count('pickup_day').alias('count_rides'), f.avg('categoryNumber').alias('avg_cat')))
    
    win = Window.partitionBy("zip", "pickup_month").orderBy(f.desc("avg_cat"), f.desc('count_rides'))
    aggDF = aggDF.withColumn("row_num", f.row_number().over(win)).where("row_num <= 10")
    
    aggDF.show()
    return aggDF

if __name__ == '__main__':
    spark = SparkSession.builder.appName("TransformJob").getOrCreate()
    taxiDF = get_taxi_df()
    zipDF = get_zip_code_mapping_df()
    airQualityDF = get_air_quality_df(zipDF)
    airQualityDF.show()
    aggedDF = calculate_hottest_days(taxiDF, airQualityDF, zipDF)
    aggedDF.cache()
    aggedDF.show()



