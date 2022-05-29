from copy import copy
import random
from random import randint, choice

from faker import Faker
import util

fake = Faker()


from faker.providers import BaseProvider
class DescriptionProvider(BaseProvider):
    def description(self):
        species = fake.words(ext_word_list=util.species_list, nb=1)[0]
        species_part = species.split(" ")[choice([0,-1])]
        word_list = fake.words(nb=10)
        words_only = ' '.join(word_list)
        word_list_with_species = copy(word_list)
        word_list_with_part = copy(word_list)
        word_list_with_species.insert(choice([0, int(len(word_list)/2), len(word_list)]), species)
        word_list_with_part.insert(choice([0, int(len(word_list)/2), len(word_list)]), species_part)
        with_species  = ' '.join(word_list_with_species)
        with_part = ' '.join(word_list_with_part)
        cases = [('', ""), (species, species), (species_part, ""), (words_only, ""), (with_species, species), (with_part, "")]
        return choice(cases)

    def description_only(self):
        species = fake.words(ext_word_list=util.species_list, nb=1)[0]
        species_part = species.split(" ")[choice([0,-1])]
        word_list = fake.words(nb=10)
        words_only = ' '.join(word_list)
        word_list_with_species = copy(word_list)
        word_list_with_part = copy(word_list)
        word_list_with_species.insert(choice([0, int(len(word_list)/2), len(word_list)]), species)
        word_list_with_part.insert(choice([0, int(len(word_list)/2), len(word_list)]), species_part)
        with_species  = ' '.join(word_list_with_species)
        with_part = ' '.join(word_list_with_part)
        cases = ['', species, species_part, words_only, with_species, with_part]
        return choice(cases)

fake.add_provider(DescriptionProvider)

def create_mock_data(length=1):
    mock_data = []
    expected = []
    for _ in range(length):
        description, species = fake.description()
        email = fake.email()
        mock_data.append(
            {
                "user": email,
                "location": fake.local_latlng(),
                "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
                "description": description,
                "count": random.randint(0, 20),
            }
        )

        expected.append(
            {
                "user": email,
                "species": species
            }
        )
    return (mock_data, expected)

