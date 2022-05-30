

from unittest import mock

import cloud_examples


@mock.patch('cloud_examples.storage', autospec=True)
def test_delete_temp(storage):
    blob = mock.Mock()
    blob.delete = mock.Mock()
    mock_bucket = storage.Client.return_value.get_bucket.return_value
    mock_bucket.list_blobs.return_value = [blob, blob]

    cloud_examples.delete_temp("fake_bucket", "fake_prefix")

    storage.Client.assert_called_once()
    client_mock = storage.Client.return_value
    client_mock.get_bucket.assert_called_with("fake_bucket")
    client_mock.get_bucket.return_value.list_blobs.assert_called_with("fake_prefix")
    assert blob.delete.call_count == 2


from cloud_examples import delete_temp_aws
def test_delete_temp_aws(s3):
    s3.create_bucket(Bucket="fake_bucket")
    s3.put_object(Bucket="fake_bucket", Key="fake_prefix/something", Body=b'Some info')

    obj_response = s3.list_objects_v2(Bucket="fake_bucket", Prefix="fake_prefix")
    assert len(obj_response['Contents']) == 1

    delete_temp_aws("fake_bucket", "fake_prefix")

    obj_response = s3.list_objects_v2(Bucket="fake_bucket", Prefix="fake_prefix")
    assert obj_response['KeyCount'] == 0


@mock.patch('cloud_examples.boto3', autospec=True)
def test_delete_temp_aws_mock(boto_mock):
    list_obj_response = {'Content': [{'Key': 'someobject'}]}
    boto_mock.client.return_value.list_objects_v2.return_value = list_obj_response
