from google.cloud import storage

def delete_temp(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix)
    for blob in blobs:
        blob.delete()


import boto3

def delete_temp_aws(bucket_name, prefix):
    s3 = boto3.client('s3', region_name='us-east-1')
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    object_keys = [{'Key':item['Key']} for item in objects['Contents']]
    s3.delete_objects(Bucket=bucket_name, Delete={'Objects':object_keys})
    
