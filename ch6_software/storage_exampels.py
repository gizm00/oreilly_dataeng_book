import abc
from datetime import datetime
import gzip
import os

import json
import boto3
# import sqlalchemy as sa
from google.cloud import storage

from ch6_software.cloud_storage import CloudStorage
from common import species_list

def aggregate_by_species(species_list):
    print(species_list)

def write_to_s3(data, key):
    s3 = boto3.resource("s3")
    object = s3.Object("bucket_name", key)
    object.put(Body=data)

def write_to_s3(data, key):
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.put_object(Bucket="bucket_name", Key=key, Body=data)


def write_to_s3(data, key):
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.put_object(Bucket="bucket_name", Key=key, Body=gzip.compress(data))

def write_to_gcs(data):
    pass



# Tightly coupled
def create_aggregate_data(data):
    species_list = ["night heron", "great blue heron", "grey heron", "whistling heron"]
    bird_agg  = aggregate_by_species(data, species_list)
    s3 = boto3.client('s3', region_name='us-east-1')
    key = f"aggregate_data_{datetime.utcnow().isoformat()}.json"
    json_str = json.dump(bird_agg)
    json_bytes = json_str.encode('utf-8') 
    s3.put_object(Bucket="bucket_name", Key=key, Body=json_bytes)

def aggregate_by_location():
    pass


# Less tightly coupled
def create_aggregate_data(data):
    species_list = ["night heron", "great blue heron", "grey heron", "whistling heron"]
    bird_agg  = aggregate_by_species(data, species_list)
    key = f"aggregate_data_{datetime.utcnow().isoformat()}.json"
    write_to_s3(bytes(json.dumps(bird_agg)), key)

# species_agg = create_aggregate_data(data)
# with_social = data_with_location(data)
# write_to_s3(species_agg, "species_data")
# write_to_s3(with_social, "social_data")

# multi cloud


class ProcessBirdData:
    def __init__(self, storage -> ObjectStorage):
        self.storage = storage

    @classmethod
    def create_aggregate_data(data):
        species_list = ["night heron", "great blue heron", "grey heron", "whistling heron"]
        return aggregate_by_species(species_list, data)
    
    def run(self, data):
        bird_agg = create_aggregate_data(data)
        self.storage.add(bytes(json.dumps(bird_agg)), "species_data")


# storage_platform = CloudStorage(os.getenv('DEPLOYMENT'))
# data = acquire_data()
# bird_data_process = ProcessBirdData(storage_platform)
# bird_data_process.run(data)


def test_species_label(spark_context):
    data = [{'description': "Saw a night heron"}]
    data_df = spark_context.parallelize(data).toDF()
    species_list = ['night heron']
    result_df = apply_species_label(species_list, data_df)
    result = result_df.toPandas().to_dict('list')
    assert result['species'][0] == 'night heron'

r_species = f".*({'|'.join(species_list)}).*"

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
         .json("ch6_software/result.json")a
    )

def apply_species_label(species_list, df):
    r_species = f".*({'|'.join(species_list)}).*"
    return df.withColumn(
            "species",
                f.when(f.col("species_conf")) > 0.8, f.col("ml_species")
                f.otherwise(
                    .withColumn("description_lower", f.lower('description'))
                    .withColumn("species", f.regexp_extract('description_lower', r_species, 1))
                    .drop("description_lower")
                )
            )
        

def get_zip(df):
    # get zipcode, return dataframe
    pass
    
class Model:
    def hello(self):
        print("hello", self.name)

from sqlalchemy import create_engine, Session

class DatabaseStorage(AbstractStorage):
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
 
    def add(self, data, id, model_cls):
        data['id'] = id
        with Session(self.engine) as session:
            model_inst = model_cls(**data)
            session.add(model_inst)
            session.commit()


from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()
class User(Base):
    __tablename__ = "aggregate_data"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    species = Column(String)



# Exercise 1 possible answer

df = (spark
        .read
        .json('ch6_software/test_data.json')
        .withColumn("zipcode", f.slice(f.col("location"), 2, 1))
        .transform(get_zip)
        .withColumn("description_lower", f.lower('description'))
        .withColumn("species", f.regexp_extract('description_lower', species_regex, 1))
        .drop("description_lower")
        .groupBy(["timezone","species"])
        .agg({"count":"sum"}))