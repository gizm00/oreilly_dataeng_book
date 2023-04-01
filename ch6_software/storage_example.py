import abc
from datetime import datetime
import gzip

import json
import boto3
from sqlalchemy import create_engine, Session

from common import species_list

def aggregate_by_species(data, species_list):
    pass

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

# multi cloud
class ProcessBirdData:
    def __init__(self, storage -> AbstractStorage):
        self.storage = storage

    @classmethod
    def create_aggregate_data(data):
        species_list = ["night heron", "great blue heron", "grey heron", "whistling heron"]
        return aggregate_by_species(species_list, data)
    
    def run(self, data):
        bird_agg = create_aggregate_data(data)
        self.storage.add(bytes(json.dumps(bird_agg)), "species_data")
    
@abc
class AbstractStorage:
   def add(data, id):
       raise NotImplementedError

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
