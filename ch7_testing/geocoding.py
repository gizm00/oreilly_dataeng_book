from lib2to3.pytree import Base
import requests

# not a real API! At least as far a I know...
GEOCODING_API = "http://www.geocoding.com/get_zipcode"

class GeocodingError(BaseException):
    pass

# add tenacity retry
def get_zip(lat_long):
    response  = requests.get(GEOCODING_API)
    if response.status_code != 200:
        raise GeocodingError(f"Unable to get zipcode for {lat_long}")
    return response.json()
