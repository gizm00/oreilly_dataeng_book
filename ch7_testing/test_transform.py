from this import d
import faker_example
import manual_example
import transform

from hypothesis import given, settings
from hypothesis import strategies as st
import string
import pytest
from pyspark.sql import SparkSession

import util
from hypothesis_example import description

# run pytest with -s to see the test cases printed during execution

# The first time the spark fixture is used, the startup time will be
# part of the test time. Without deadline=None, hypothesis will recognize
# the outlier runtime and mark it as an error, as it can be indicative of
# flaky test behavior.
# https://hypothesis.readthedocs.io/en/latest/settings.html?highlight=deadline#hypothesis.settings.deadline
@settings(deadline=None)
@given(description(), st.emails())
def test_transform_hypothesis(spark_context, description, emails):
    species = description[0]
    desc = description[1]

    test_df = util.df_from_list_dict([{'description': desc, 'user': emails}], spark_context)
    expected_df = util.df_from_list_dict([{'species': species, 'user': emails}], spark_context)
    df_with_species = transform.apply_species_label(util.species_list, test_df)
    print("description:", desc, "species:", species)
 
    assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()

def test_transform_manual(spark_context):
    data, expected = manual_example.create_mock_data()

    test_df = util.df_from_list_dict(data, spark_context)
    expected_df = util.df_from_list_dict(expected, spark_context)
    df_with_species = transform.apply_species_label(util.species_list, test_df)

    assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()

def test_transform_faker(spark_context):
    # 4 is not necessarily enough to get a random sample of species + empty
    data, expected = faker_example.create_mock_data(4)

    test_df = util.df_from_list_dict(data, spark_context)
    expected_df = util.df_from_list_dict(expected, spark_context)
    df_with_species = transform.apply_species_label(util.species_list, test_df)
    print(df_with_species.show(4, False))
    print(expected_df.show(4, False))
    assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()