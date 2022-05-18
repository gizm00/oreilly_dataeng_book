import random

from faker import Faker
import util

fake = Faker()


def create_mock_data(length=1):
    mock_data = []
    expected = []
    for _ in range(length):
        add_species = fake.pybool()
        user = fake.email()
        if add_species:
            species = fake.words(ext_word_list=util.species_list, nb=1)
            description = ' '.join(fake.words(nb=5) + species + fake.words(nb=5))
        else:
            species = '' # Empty string is what regexp_match will return if no match
            description = [' '.join(fake.words(nb=10)), ''][random.randint(0,1)]
        expected.append({
            "user": user,
            "species": species[0] if species != '' else ''
        })
        mock_data.append(
            {
                "user": user,
                "location": fake.local_latlng(),
                "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
                "description": description,
                "count": random.randint(0, 20),
            }
        )
    return mock_data, expected


### code for pretty bookness

# from faker import Faker
# fake = Faker()
# mock_data = {
#         "user": fake.email(),
#         "location": fake.local_latlng(),
#         "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
#         "description": f"{' '.join(fake.words(nb=10))}",
#         "count": random.randint(0, 20),
#     }


from random import randint, choice
def create_mock_data(length=1):
    mock_data = []
    for _ in range(length):
        species = fake.words(ext_word_list=species_list, nb=randint(0,1))
        word_list = fake.words(nb=choice([0,5,10]))
        if species:
            species = [species, species[choice([0,-1])]][randint(0,1)]
        word_list.insert(choice([0, int(len(word_list)/2), len(word_list)]), species)
        description = ' '.join(word_list)
        # description = [description, ''][random.randint(0,1)]

        mock_data.append(
            {
                "user": fake.email(),
                "location": fake.local_latlng(),
                "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
                "description": description,
                "count": random.randint(0, 20),
            }
        )
    return mock_data


from faker.providers import BaseProvider
class DescriptionProvider(BaseProvider):
    def description(self):
        species = fake.words(ext_word_list=species_list, nb=1)[0]
        species_part = species.split(" ")[choice([0,-1])]
        word_list = fake.words(nb=10)
        words_only = ' '.join(word_list)
        word_list_with_species = word_list
        word_list_with_part = word_list
        word_list_with_species.insert(choice([0, int(len(word_list)/2), len(word_list)]), species)
        word_list_with_part.insert(choice([0, int(len(word_list)/2), len(word_list)]), species_part)
        with_species  = ' '.join(word_list_with_species)
        with_part = ' '.join(word_list_with_part)
        cases = ['', species, species_part, words_only, with_species, with_part]
        return choice(cases)

fake.add_provider(DescriptionProvider)

def description_gen():
    species = fake.words(ext_word_list=species_list, nb=randint(0,1))
    word_list = fake.words(nb=choice([0,10]))
    if species:
        species = [species[0], species[0].split(" ")[choice([0,-1])]][randint(0,1)]
        word_list.insert(choice([0, int(len(word_list)/2), len(word_list)]), species)
    return ' '.join(word_list)