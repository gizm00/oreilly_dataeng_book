# from google.cloud import storage
class Fake:
    pass

storage = Fake()

def delete(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        blob.delete()