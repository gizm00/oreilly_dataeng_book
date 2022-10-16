from copy import copy
import random
from random import randint, choice, choices

from faker import Faker
import util
import manual_example

fake = Faker()

def basic_fake_data():
    fake = Faker()
    fake_data = {
        "user": fake.email(), # fake.uuid4(),
        "location": fake.local_latlng(),
        "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}"],
        "description": f"{' '.join(fake.words(nb=10))}",
        "count": random.randint(0, 20),
    }
    for k,v in fake_data.items():
        print(k, ":", v)


from faker.providers import BaseProvider

from string import digits
def generate_id():
    """
    Provide a random Survey Data ID
    format: bird-surv-10243
    """
    id_values = random.sample(digits, 5)
    return f"bird-surv-{''.join(id_values)}"

def create_test_data_ids(length=10):
    mock_data = []
    for _ in range(length):
        description, species = fake.description() 
        email = fake.email()
        mock_data.append(
            {
                "id": generate_id(),
                "user": email,
                "location": fake.local_latlng(),
                "img_files": [f"s3://bucket-name{fake.file_path(depth=2)}" for i in range(random.randint(0,3))],
                "description": description, 
                "count": random.randint(0, 20),
            }
        )

class DescriptionProvider(BaseProvider):
    def description_cases(self):
        species = fake.words(ext_word_list=util.species_list, nb=1)[0]
        species_part = species.split(" ")[choice([0,-1])] # choose 2
        species_use = choice([species, species_part, ""]) # choose 3
        word_list = fake.words(nb=choice([0,10])) # choose 2
        index = choice([0, len(word_list)-1, int((len(word_list)-1)/2)]) # choose 3
        if len(word_list) > 0:
            return ' '.join(word_list[0:index] + [species_use] + word_list[index+1:])
        else:
            return species_use

    def description(self):
        species = fake.words(ext_word_list=util.species_list, nb=randint(0,1))
        word_list = fake.words(nb=10)
        index = randint(0, len(word_list)-1)
        description = ' '.join(word_list[0:index] + species + word_list[index+1:])
        return description

    def description_species(self):
        species = fake.words(ext_word_list=util.species_list, nb=randint(0,1))
        word_list = fake.words(nb=10)
        index = randint(0, len(word_list)-1)
        description = ' '.join(word_list[0:index] + species + word_list[index+1:])
        return (description, ''.join(species))

    def description_distribution_expected(self):
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
        weights = [10, 2, 3, 5, 6, 7]
        return choices(cases, weights=weights)[0]

    def description_distribution(self):
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
        weights = [10, 2, 3, 5, 6, 7]
        return choices(cases, weights=weights)[0]

fake.add_provider(DescriptionProvider)

def create_test_data(length=1):
    """
    Create mock data cases and expected results
    Returns (mock_data->list, expected->list)
    """
    mock_data = []
    expected = []
    for _ in range(length):
        description, species = fake.description_species() 
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

def create_test_data_distributed(length=1):
    """
    Create mock data cases and expected results
    Returns (mock_data->list, expected->list)
    """
    mock_data = []
    expected = []
    for _ in range(length):
        description, species = fake.description_distribution_expected()
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

def create_test_data_with_manual(length):
    data, expected = manual_example.create_test_data()
    auto_data, auto_expected = create_test_data(length)
    return (data+auto_data, expected+auto_expected)

def description_graph(weights=None):
    cases = ['empty', 'species', 'species_part', 'words_only', 'with_species', 'with_part']
    weights = [10, 2, 3, 5, 6, 7] if weights else []
    return choices(cases)[0]



if __name__ == '__main__':
    import json
    test_data = create_test_data(40)
    with open('test_data.json', 'w+') as f:
        json.dump(test_data, f)