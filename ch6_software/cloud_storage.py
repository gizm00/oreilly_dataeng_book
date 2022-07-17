import abc
from datetime import datetime
import gzip

import json
import boto3
import sqlalchemy as sa
from google.cloud import storage

# from ch6_software.something import ProcessBirdData

class ObjectStorage(abc.ABC):
   def add(data, id):
       raise NotImplementedError

class S3Storage(ObjectStorage):
    def __init__(self, bucket):
       self.s3 = boto3.client('s3', region_name='us-east-1')
       self.bucket = bucket
 
    def add(self, data, id):
       self.s3.put_object(Bucket=self.bucket, Key=id, Body=data)


class GCSStorage(ObjectStorage):
    def __init__(self, bucket):
        self.gcs = storage.Client()
        self.bucket = self.gcs.bucket(bucket)

    def add(self, data, id):
        blob = self.bucket.blob(id)
        blob.upload_from_string(json.dumps(data))

class MockStorage(ObjectStorage):
    def __init__(self, bucket):
        self.bucket = bucket

    def add(self, data, id):
        print(f"Would have written {data} to {self.bucket} at {id}")
        return (data, f"{self.bucket}/{id}")


class CloudStorage(ObjectStorage):
    def __init__(self, deploy):
        if deploy == 'gcs':
            self.cloud_storage = GCSStorage()
        elif deploy == 'aws':
            self.cloud_storage = S3Storage()

    def add(self, data, id):
        self.cloud_storage(data, id)


def test_bird_data_process(test_data, expected_data, expected_path):
    storage = MockStorage("bucket_name")
    bird_data_process = ProcessBirdData(storage)
    result, object_path = bird_data_process(test_data)
    assert result == expected_data
    assert object_path == expected_path

