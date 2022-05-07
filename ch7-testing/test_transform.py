
import faker_example as faker_ex
import manual_example as manual
from transform import apply_species_label
from util import df_from_list_dict, species_list

def test_transform_manual():
    data, expected = manual.create_fake_data()

    test_df = df_from_list_dict(data)
    expected_df = df_from_list_dict(expected)
    df_with_species = apply_species_label(species_list, test_df)

    assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()

def test_transform_faker():
    data, expected = faker_ex.create_fake_data(4)

    test_df = df_from_list_dict(data)
    expected_df = df_from_list_dict(expected)
    df_with_species = apply_species_label(species_list, test_df)

    assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()