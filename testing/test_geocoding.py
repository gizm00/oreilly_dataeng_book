import json
from unittest import mock
import responses
from responses.registries import OrderedRegistry
import geocoding

import pytest
from geocoding import get_zip, get_zip_retry, lat_long_to_pop, GeocodingError, GeocodingRetryException

@mock.patch('geocoding.get_zip', mock.Mock(return_value='95472'))
@mock.patch('geocoding.get_population', mock.Mock(return_value='1000'))
def test_lat_long_to_pop():
    assert lat_long_to_pop((38.4021, 122.8239)) == {'95472':'1000'}

class MockResponse:
   def __init__(self, json_data, status_code):
       self.json_data = json_data
       self.status_code = status_code
 
   def json(self):
       return self.json_data


# For illustrating conftest create fixutres on gather, make a list of 
# status code to return values and parametrize
# Can this mock be a fixture? maybe same deal
@mock.patch('geocoding.requests', autospec=True)
def test_get_zip_404(mock_requests):
    mock_requests.get.return_value.status_code = 404
    mock_requests.get.return_value.json.return_value = {}

    with pytest.raises(GeocodingError):
        get_zip((38.4021, 122.8239))
    
@mock.patch('geocoding.requests', autospec=True)
def test_get_zip_ok(mock_requests):
    mock_requests.get.return_value.status_code = 200
    mock_requests.get.return_value.json.return_value = {"zipcode": "95472"}
    assert get_zip((38.4021, 122.8239)) == "95472"
    mock_requests.get.assert_called_once_with(
                url=geocoding.GEOCODING_API, 
                params={"lat_long": (38.4021, 122.8239)}
            )


@mock.patch('geocoding.requests', autospec=True)
def test_get_zip_retry_mock(mock_requests):
    get_zip_retry.retry.sleep = mock.Mock()
    resp_429 = MockResponse({}, 429)
    resp_200 = MockResponse({"zipcode": "95472"}, 200)
    responses = [resp_429, resp_429, resp_200]
    mock_requests.get.side_effect = responses
    zip = get_zip_retry((38.4021, 122.8239))
    assert zip == "95472"
    assert get_zip_retry.retry.statistics.get('attempt_number') == 3

def test_get_poplation():
    zip_resp = MockResponse({"zipcode": "95472"}, 200)
    pop_resp = MockResponse({"population": "17558"}, 200)
    with mock.patch('requests.get', side_effect=[zip_resp, pop_resp]):
        population = lat_long_to_pop((38.4021, 122.8239))

    assert population["95472"] == "17558"


#########
# Examples using responses
#########
@responses.activate()
def test_get_zip_ok_resp():
    zip_resp = responses.get(
        geocoding.GEOCODING_API, 
        #"www.python.org",
        status=200, 
        json={"zipcode": "95472"})
    assert get_zip((38.4021, 122.8239)) == "95472"
    assert zip_resp.call_count == 1




# Ordered registry ensures responses are called in order, which we want
# here to simulate the API responding with 200 after a few tries
@responses.activate(registry=OrderedRegistry)
def test_get_zip_retry():
    get_zip_retry.retry.sleep = mock.Mock()
    responses.get(geocoding.GEOCODING_API, status=429, json={})
    responses.get(geocoding.GEOCODING_API, status=429, json={})
    responses.get(geocoding.GEOCODING_API, status=200, json={"zipcode": "95472"})
    zip = get_zip_retry((38.4021, 122.8239)) 
    assert zip == "95472"
    assert get_zip_retry.retry.statistics.get('attempt_number') == 3

@responses.activate()
def test_get_zip_retries_exhausted():
    get_zip_retry.retry.sleep = mock.Mock()
    for _ in range(6):
        responses.get(geocoding.GEOCODING_API, status=429, json={})
        
    with pytest.raises(GeocodingRetryException):
        get_zip_retry((38.4021, 122.8239)) 
    assert get_zip_retry.retry.statistics.get('attempt_number') == 5


