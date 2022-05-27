

from unittest import mock

import cloud_examples


@mock.patch('cloud_examples.storage', autospec=True)
def test_delete_temp(storage, client_mock):
    storage.Client.return_value = client_mock

    cloud_examples.delete_temp("fake_bucket", "fake_prefix")

    storage.Client.assert_called_once()
    client_mock.get_bucket.assert_called_with("fake_bucket")
    client_mock.get_bucket.return_value.list_blobs.assert_called_with("fake_prefix")
    blobs = client_mock.get_bucket.return_value.list_blobs.return_value
    assert blobs[0].delete.call_count == 2