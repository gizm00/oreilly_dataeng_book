
from .util import species_list

def create_mock_data():
    mock_data = [
        {
            "user": "something@email.com",
            "location": "45.12431, 121.12453",
            "img_files": ["s3://bucket-name/file.png"],
            "description": "there was a night heron",
            "count": 1,
        },
        {
            "user": "anotherthing@email.com",
            "location": "45.12431, 121.12453",
            "img_files": [],
            "description": "",
            "count": 10,
        },
        {
            "user": "third@email.com",
            "location": "45.12431, 121.12453",
            "img_files": [],
            "description": "there was a heron",
            "count": 1,
        },
    ]

    expected = [
        {"user": "something@email.com", "species": "night heron"},
        {"user": "anotherthing@email.com", "species": ""},
        {"user": "third@email.com", "species": ""},  
    ]

    return mock_data, expected