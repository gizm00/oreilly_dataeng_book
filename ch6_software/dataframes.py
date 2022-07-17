from pyspark.sql import functions as f

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql import functions as f

spark = (SparkSession
         .builder
         .appName('oreilly-book')
         .getOrCreate())

species_list = ["night heron", "great blue heron", "grey heron", "whistling heron"]
species_regex = f".*({'|'.join(species_list)}).*"

def apply_species_label(species_list, df):
    return (df
        .withColumn("description_lower", f.lower('description'))
        .withColumn("species", f.regexp_extract('description_lower', species_regex, 1))
        .drop("description_lower")
    )

# 1. doing it all at once
df = (spark
         .read
         .json('ch6_software/test_data.json')
         .withColumn("description_lower", f.lower('description'))
         .withColumn("species", f.regexp_extract('description_lower', species_regex, 1))
         .drop("description_lower")
         .groupBy("species")
         .agg({"count":"sum"})
         .write
         .mode("overwrite")
         .json("ch6_software/result.json")
    )

# Show issues with 1
# difficult to test - transformations in isolation
# limited to reading / writing as inspection mehcanism
# adding new transformations touches existing code

# 2. breaking apart transformations
def get_json(file_name):
    return spark.read.json(file_name)

def apply_species_label(df):
    species_list = ["night heron", "great blue heron", "grey heron", "whistling heron"]
    species_regex = f".*({'|'.join(species_list)}).*"
    return (df
        .withColumn("description_lower", f.lower('description'))
        .withColumn("species", f.regexp_extract('description_lower', species_regex, 1))
        .drop("description_lower")
    )

def create_aggregate_data(df):
    return (df
        .groupBy("species")
        .agg({"count":"sum"})
        )

def store_json(df, file_name):
    (df.write
        .mode("overwrite")
        .json(file_name))

df = (get_json('ch6_software/test_data.json')
        .transform(apply_species_label, species_list)
        .transform(create_aggregate_data)
        .transform(store_json('ch6_software/result_chain.json'))
)