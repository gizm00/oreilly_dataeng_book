import json
import pytest


import boto3
import env
from cloud_examples import hod_has_night_heron_data
from conftest import aws_credentials
from moto import mock_s3

latest_prefix = "20220530T112300"

@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3')

@pytest.fixture(scope="function")
def s3_with_bucket(s3):
    s3.create_bucket(Bucket=env.ENRICHED_DATA_BUCKET)
    yield s3

@pytest.fixture(scope="function")
def s3_w_species(s3_with_bucket):
    data = [{'id': '23435', 'species': 'night heron'}]
    data_str = json.dumps(data)
    s3_with_bucket.put_object(Bucket=env.ENRICHED_DATA_BUCKET, 
            Key=f"{latest_prefix}/something.json", 
            Body=bytes(data_str, 'utf-8'))

    yield s3_with_bucket

@pytest.fixture(scope="function")
def s3_wo_species(s3_with_bucket):
    data = [{'id': '23435', 'species': 'whistling heron'}]
    data_str = json.dumps(data)
    s3_with_bucket.put_object(Bucket=env.ENRICHED_DATA_BUCKET, 
            Key=f"{latest_prefix}/something.json", 
            Body=bytes(data_str, 'utf-8'))

    yield s3_with_bucket

# @pytest.mark.parametrize("s3_mock, expected", [
#     (s3_w_species, True),
#     (s3_wo_species, False)
# ])
def test_hod_has_night_heron_data(s3_w_species):
    objects = s3_w_species.list_objects_v2(Bucket=env.ENRICHED_DATA_BUCKET)
    assert objects

    assert hod_has_night_heron_data(latest_prefix) == True

def test_hod_has_night_heron_data_false(s3_wo_species):
    objects = s3_wo_species.list_objects_v2(Bucket=env.ENRICHED_DATA_BUCKET)
    assert objects

    assert hod_has_night_heron_data(latest_prefix) == False
