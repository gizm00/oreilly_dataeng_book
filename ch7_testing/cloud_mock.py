from unittest import mock
from unittest.mock import Mock
import logging

logger = logging.getLogger(__name__)

class cloud_sdk:
    pass


class CloudStorage():
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        
    def get_bucket(self):
        if not self.bucket:
            self.bucket = cloud_sdk.bucket(self.bucket_name)
        return self.bucket

    def delete(self, prefix):
        self.bucket.delete(prefix)

class CloudInteractions:
    def __init__(self, bucket):
        self.bucket_name = bucket
        self.storage = None

    @property
    def storage(self):
        if not self.storage:
            self.storage = CloudStorage(self.bucket_name)
        return self.storage

    def delete_from_cloud(self, prefix):
        self.storage.delete(prefix)

    

class MockCloudStorage():
    def __init__(self, bucket_name):
        self.bucket = bucket_name

    def delete(self, filename):
        logger.info("Would have deleted %s/%s", self.bucket, filename)

from copy import copy
# client = MockCloudStorage("fake_bucket")
blob1 = Mock()
blob1.delete = Mock()
blob2 = copy(blob1)
mock_bucket = Mock()
mock_bucket.list_blobs = Mock()
mock_bucket.list_blobs.return_value = [blob1, blob2]
client = Mock()
client.get_bucket = Mock()
client.get_bucket.return_value = mock_bucket
storage = Mock()
storage.Client = Mock()
storage.Client.return_value = client

import cloud_examples

def test_delete():
    with mock.patch.object(cloud_examples, 'storage', storage):
        cloud_examples.delete("fake_bucket", "fake_prefix")
        storage.Client.assert_called_once()
        sc = storage.Client()
        sc.get_bucket.assert_called_once()
        bucket = sc.get_bucket()
        bucket.list_blobs.assert_called_once()
        for blob in bucket.list_blobs():
            blob.delete.assert_called_once()

