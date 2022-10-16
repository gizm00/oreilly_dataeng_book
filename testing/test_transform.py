import faker_example
import manual_example
import transform
import mock_from_schema

from hypothesis import given, settings
from hypothesis import strategies as st
import string
import pytest
from pyspark.sql import SparkSession

import util
from hypothesis_example import description


# run pytest with -s to see the test cases printed during execution

def compare_results(expected, result):
    for case in expected:
        user = case['user']
        species = case['species']
        result_case = [r for r in result if r['user'] == user]
        if len(result_case) == 0:
            pytest.fail(f"Expected case: {case}. Could not find user {user} in result {result}")
        print(f"Result: {result_case}")
        print(f"Expected: {case}")
        assert result_case[0]['species'] == species

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
    test_df = spark_context.parallelize([{'description': desc, 'user': emails}]).toDF()
    expected_df = spark_context.parallelize([{'species': species, 'user': emails}]).toDF()
    df_with_species = transform.apply_species_label(util.species_list, test_df)
    print("description:", desc, "species:", species)
 
    assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()


def test_transform_manual(spark_context):
    """
    Test apply_species label with a few rows of hand curated test
    data.
    """
    data, expected = manual_example.create_test_data()
    test_df = spark_context.parallelize(data).toDF()
    df_with_species = transform.apply_species_label(util.species_list, test_df)
    with_species = df_with_species.toPandas().to_dict('records')
    compare_results(expected, with_species)


def fake_ids(value):
    if 'species' in str(value):
        return value


def test_transform_faker(spark_context):
    data, expected = faker_example.create_test_data(100)

    test_df = spark_context.parallelize(data).toDF()
    df_with_species = transform.apply_species_label(util.species_list, test_df)
    with_species = df_with_species.toPandas().to_dict('records')
    compare_results(expected, with_species)

def test_transform_faker_distributed(spark_context):
    data, expected = faker_example.create_test_data_distributed(100)

    test_df = spark_context.parallelize(data).toDF()
    df_with_species = transform.apply_species_label(util.species_list, test_df)
    with_species = df_with_species.toPandas().to_dict('records')
    compare_results(expected, with_species)


from schemas import survey_data
def test_transform_schema():
    data, expected = mock_from_schema.generate_data_expected(survey_data, 10)
    df_with_species = transform.apply_species_label(util.species_list, data)
    df_with_species.select("user", "description", "species").show(10, False)
    with_species = df_with_species.toPandas().to_dict('records')
    compare_results(expected, with_species)
    


