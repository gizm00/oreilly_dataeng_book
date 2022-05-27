## fake data generators as python fixtures for using in tests
import logging
from unittest import mock
from unittest.mock import Mock
import pytest
from pyspark.sql import SparkSession

import faker_example

# Starting a spark app is a significant overhead, first test that uses this fixture
# will take a while to run ~ 20s
# pyspark unit testing reference: https://blog.cambridgespark.com/unit-testing-with-pyspark-fb31671b1ad8
def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .master('local[2]')
             .appName('pytest-pyspark-local-testing')
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    spark.sparkContext.addPyFile("/Users/gizmo/dev/oreilly_dataeng_book/ch7_testing/util.py")
    return spark.sparkContext

# try and work this in using mock API responses, where i want to filter
# on response code in the test at runtime
# def pytest_generate_tests(metafunc):
#     data, expected = faker_example.create_mock_data(34)
#     if "faker_data" in metafunc.fixturenames:
#         metafunc.parametrize("faker_data, expected", zip(data,expected))


@pytest.fixture
@mock.patch('cloud_examples.storage', autospec=True)
def storage_mock(storage):
    blob = Mock()
    blob.delete = Mock()
    mock_bucket = Mock()
    mock_bucket.list_blobs = Mock()
    mock_bucket.list_blobs.return_value = [blob, blob]
    client = Mock()
    client.get_bucket = Mock()
    client.get_bucket.return_value = mock_bucket
    storage.Client.return_value = client
    yield storage

@pytest.fixture
def client_mock():
    blob = Mock()
    blob.delete = Mock()
    mock_bucket = Mock()
    mock_bucket.list_blobs = Mock()
    mock_bucket.list_blobs.return_value = [blob, blob]
    client = Mock()
    client.get_bucket = Mock()
    client.get_bucket.return_value = mock_bucket
    return client
