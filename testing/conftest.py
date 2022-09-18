## fake data generators as python fixtures for using in tests
from enum import auto
import logging
import os
from pickletools import pyset
from unittest import mock
from unittest.mock import Mock

import boto3
from moto import mock_s3
import pytest
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

import faker_example

# Starting a spark app is a significant overhead, first test that uses this fixture
# will take a while to run ~ 20s
# pyspark unit testing reference: https://blog.cambridgespark.com/unit-testing-with-pyspark-fb31671b1ad8
def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)

# For illustration only - do not store passwords in code files, in production
# these values should come from a credentials store
TEST_DB_INFO = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "pass": "postgres",
    "db_name": "postgres"
}

def setup_test_db():
    # some typical connection strings, assuming postgres runs in a container:
    # code running locally: postgresql://user:pass@localhost:5432/db_name
    # code running in a container: "postgresql://user:pass@host.docker.internal:5432/db_name"
    creds = f"{TEST_DB_INFO['user']}:{TEST_DB_INFO['pass']}"
    host = f"{TEST_DB_INFO['host']}:{TEST_DB_INFO['port']}"
    engine = create_engine(f"postgresql://{creds}@{host}", isolation_level='AUTOCOMMIT')
    with engine.connect() as conn:
        conn.execute("DROP DATABASE IF EXISTS test_db")
        conn.execute("CREATE DATABASE test_db")
        conn.close()
        return engine

def teardown_test_db(engine):
    with engine.connect() as conn:
        conn.execute("DROP DATABASE test_db")
    engine.dispose()

@pytest.fixture(scope="session")
def test_db():
    creds = f"{TEST_DB_INFO['user']}:{TEST_DB_INFO['pass']}"
    host = f"{TEST_DB_INFO['host']}:{TEST_DB_INFO['port']}"
    engine = setup_test_db()
    test_engine = create_engine(f"postgresql://{creds}@{host}/test_db")
    test_conn = test_engine.connect()
    yield test_conn
    test_conn.close()
    test_engine.dispose()
    teardown_test_db(engine)



@pytest.fixture(scope="session")
def treatment_conn(test_db):
    test_db.execute("""
        CREATE TABLE treatment (
            id int,
            name text
        )
    """)
    test_db.execute("""
        INSERT INTO treatment VALUES(1, 'Drug A');
        INSERT INTO treatment VALUES(2, 'Drug C');
    """)
    yield test_db


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
    spark.sparkContext.addPyFile("./util.py")
    return spark.sparkContext

# try and work this in using mock API responses, where i want to filter
# on response code in the test at runtime
# def pytest_generate_tests(metafunc):
#     data, expected = faker_example.create_mock_data(34)
#     if "faker_data" in metafunc.fixturenames:
#         metafunc.parametrize("faker_data, expected", zip(data,expected))


# Suggested setup for moto
# https://docs.getmoto.org/en/latest/docs/getting_started.html#example-on-usage

@pytest.fixture(scope="function")
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture
def client_mock():
    blob = Mock()
    blob.delete = Mock()
    mock_bucket = Mock()
    mock_bucket.list_blobs = Mock()
    mock_bucket.list_blobs.return_value = [blob, blob]
    client = Mock()
    client.nothere = Mock()
    client.get_bucket = Mock()
    client.get_bucket.return_value = mock_bucket
    return client
