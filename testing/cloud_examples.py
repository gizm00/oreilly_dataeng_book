from google.cloud import storage

def delete_temp(bucket_name, prefix):
    storage_client = storage.Client() # @mock.patch('cloud_examples.storage', autospec=True)
    bucket = storage_client.get_bucket(bucket_name) # storage.Client.return_value.get_bucket.return_value

    blobs = bucket.list_blobs(prefix) # mock_bucket.list_blobs.return_value 
    for blob in blobs: # [blob, blob]
        blob.delete() # blob = mock.Mock(Blob); blob.delete.return_value = None


import boto3

def delete_temp_aws(bucket_name, prefix):
    s3 = boto3.client('s3', region_name='us-east-1')
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    object_keys = [{'Key':item['Key']} for item in objects['Contents']]
    s3.delete_objects(Bucket=bucket_name, Delete={'Objects':object_keys})
    
import env
import io
import pandas as pd

def hod_has_night_heron_data(prefix):
    s3 = boto3.client('s3', region_name='us-east-1')
    objects = s3.list_objects_v2(Bucket=env.ENRICHED_DATA_BUCKET, Prefix=prefix)
    for obj in objects['Contents']:
        content = s3.get_object(Bucket=env.ENRICHED_DATA_BUCKET, Key=obj['Key'])
        df = pd.read_json(io.BytesIO(content['Body'].read()))
        if len(df.query("species == 'night heron'")) > 0:
            return True
    return False
