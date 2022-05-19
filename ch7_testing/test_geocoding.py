import json
from unittest import mock

import pytest
from geocoding import get_zip, lat_long_to_pop, GeocodingError, GeocodingRetryException

class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

def test_get_zip_404():
    response = MockResponse({}, 404)
    with mock.patch('geocoding.requests.get', return_value=response):
        with pytest.raises(GeocodingError):
            get_zip((45.5152, 122.6784))

def test_get_zip_ok():
    response = MockResponse({"zipcode": "97201"}, 200)
    with mock.patch('requests.get', return_value=response):
        zip = get_zip((45.5152, 122.6784))
    assert zip

def test_get_zip_retry():
    resp_429 = MockResponse({}, 429)
    resp_200 = MockResponse({"zipcode": "97201"}, 200)
    get_zip.retry.sleep = mock.Mock()
    with mock.patch('geocoding.requests.get', side_effect=[resp_429, resp_429, resp_200]):
        zip = get_zip((45.5152, 122.6784)) 
    assert zip["zipcode"] == "97201"
    assert get_zip.retry.statistics.get('attempt_number') == 3

def test_get_zip_retries_exhausted():
    resp_429 = MockResponse({}, 429)
    get_zip.retry.sleep = mock.Mock()
    with mock.patch('geocoding.requests.get', side_effect=[resp_429]*6):
        with pytest.raises(GeocodingRetryException):
            get_zip((45.5152, 122.6784)) 
    assert get_zip.retry.statistics.get('attempt_number') == 5

def test_get_poplation():
    zip_resp = MockResponse({"zipcode": "97201"}, 200)
    pop_resp = MockResponse({"population": "17558"}, 200)
    with mock.patch('requests.get', side_effect=[zip_resp, pop_resp]):
        population = lat_long_to_pop((45.5152, 122.6784))

    assert population["population"] == "17558"

