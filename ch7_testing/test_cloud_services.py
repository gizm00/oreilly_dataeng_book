

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

    