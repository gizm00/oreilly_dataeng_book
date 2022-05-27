from google.cloud import storage

def delete_temp(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix)
    for blob in blobs:
        blob.delete()

 