from hypothesis import given
from hypothesis import strategies as st
import string

import pytest

from .util import species_list
names = st.text(alphabet=string.ascii_letters)

@given(names)
def test_names(names):
    print(names)

words=st.lists(st.text(alphabet=string.ascii_letters, min_size=1), min_size=3, max_size=5)
species=st.sampled_from(species_list + [''])


@st.composite
def description(draw):
    start = draw(words) 
    species = draw(species) 
    end = draw(words)
    return (species, ' '.join(start + species + end))

@given(description)
def test_description(desc):
    print(desc)
    species = desc[0]
    desc = desc[1]
    if species != '':
        assert species in desc


# import faker_example as faker_ex
# import manual_example as manual
# from transform import apply_species_label
# from util import df_from_list_dict, species_list

# def test_transform_manual():
#     data, expected = manual.create_mock_data()

#     test_df = df_from_list_dict(data)
#     expected_df = df_from_list_dict(expected)
#     df_with_species = apply_species_label(species_list, test_df)

#     assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()

# def test_transform_faker():
#     data, expected = faker_ex.create_mock_data(4)

#     test_df = df_from_list_dict(data)
#     expected_df = df_from_list_dict(expected)
#     df_with_species = apply_species_label(species_list, test_df)

#     assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()