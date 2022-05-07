import random

from faker import Faker
from util import species_list

fake = Faker()

def create_fake_data(length=1):
    fake_data = []
    expected = []
    for _ in range(length):
        add_species = fake.pybool()
        user = fake.email()
        if add_species:
            species = ' '.join(fake.words(ext_word_list=species_list, nb=random.randint(0,1)))
            description = f"{' '.join(fake.words(nb=10))} {species} {' '.join(fake.words(nb=10))}"
        else:
            species = '' # Empty string is what regexp_match will return if no match
            description = ' '.join(fake.words(nb=20))
        expected.append({
            "user": user,
            "species": species
        })
        fake_data.append(
            {
                "user": user,
                "location": fake.local_latlng(),
                "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
                "description": description,
                "count": random.randint(0, 20),
            }
        )
    return fake_data, expected


### code for pretty bookness

from faker import Faker
fake = Faker()
fake_data = {
        "user": fake.email(),
        "location": fake.local_latlng(),
        "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
        "description": f"{' '.join(fake.words(nb=10))}",
        "count": random.randint(0, 20),
    }