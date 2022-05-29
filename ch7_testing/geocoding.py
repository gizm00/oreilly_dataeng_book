import logging
import requests

import tenacity

# not a real API! At least as far a I know...
GEOCODING_API = "http://www.geocoding.com/get_zipcode"
POPULATION_API = "http://www.geocoding.com/get_population"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class GeocodingError(BaseException):
    pass

class GeocodingRetryException(BaseException):
    pass

@tenacity.retry(retry=tenacity.retry_if_exception_type(GeocodingRetryException),
       stop=tenacity.stop_after_attempt(5),
       wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
       reraise=True)
def get_zip(lat_long):
    response  = requests.get(GEOCODING_API, {"lat_long": lat_long})
    if response.status_code == 429:
        raise GeocodingRetryException()
    if response.status_code != 200:
        raise GeocodingError(f"Unable to get zipcode for {lat_long}")
    return response.json()

def get_population(zipcode):
    response  = requests.get(POPULATION_API, {"zipcode": zipcode})
    if response.status_code != 200:
        raise GeocodingError(f"Unable to get population for {zipcode}")
    return response.json()

def lat_long_to_pop(lat_long):
    try:
        zipcode = get_zip(lat_long)
    except GeocodingRetryException:
        logging.exception("Attempt to get zipcode timedout:", zipcode.retry.statistics)
        return
    return get_population(zipcode)
