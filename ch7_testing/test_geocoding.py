import json
from unittest import mock

import pytest
from geocoding import get_zip, GeocodingError

class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

def test_get_zip_404():
    response = MockResponse({}, 404)
    with mock.patch('requests.get', return_value=response):
        with pytest.raises(GeocodingError):
            get_zip((45.5152, 122.6784))

def test_get_zip_ok():
    response = MockResponse({"zipcode": "97201"}, 200)
    with mock.patch('requests.get', return_value=response):
        zip = get_zip((45.5152, 122.6784))

    assert zip
