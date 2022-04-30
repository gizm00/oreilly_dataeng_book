import random

from faker import Faker

fake = Faker()
species_list = ["night heron", "great blue heron"]


def create_fake_data(length=1):
    fake_data = []
    for _ in range(length):
        fake_data.append(
            {
                "user": fake.email(),
                "location": fake.local_latlng(),
                "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
                "description": f"{' '.join(fake.words(nb=10))} {' '.join(fake.words(ext_word_list=species_list, nb=random.randint(0,1)))} {' '.join(fake.words(nb=10))}",
                "count": random.randint(0, 20),
            }
        )
    return fake_data
