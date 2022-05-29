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

spark = (SparkSession
            .builder
            .master('local[2]')
            .appName('pytest-pyspark-local-testing')
            .enableHiveSupport()
            .getOrCreate())
# request.addfinalizer(lambda: spark.stop())

# quiet_py4j()
spark.sparkContext.addPyFile("/Users/gizmo/dev/oreilly_dataeng_book/ch7_testing/util.py")
sc = spark.sparkContext



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


def test_transform_manual():
    spark_context = sc
    data, expected = manual_example.create_mock_data()

    test_df = util.df_from_list_dict(data, spark_context)
    df_with_species = transform.apply_species_label(util.species_list, test_df)
    expected_df = util.df_from_list_dict(expected, spark_context)

    # this runs in 13s, vs 17s for df subtract
    # with_species = map(lambda row: row.asDict(), df_with_species.collect())
    # assert expected == with_species

    diff = df_with_species.select('species','user').subtract(expected_df)
    diff.show()
    assert diff.rdd.isEmpty()

def fake_ids(value):
    if 'species' in str(value):
        return value


def test_transform_faker(spark_context):
    data, expected = faker_example.create_mock_data(10)

    test_df = util.df_from_list_dict(data, spark_context)
    expected_df = util.df_from_list_dict(expected, spark_context)
    df_with_species = transform.apply_species_label(util.species_list, test_df)

    assert df_with_species.select('species','user').subtract(expected_df).rdd.isEmpty()


from schemas import survey_data
def test_transform_schema():
    data, expected = mock_from_schema.generate_data(survey_data)
    df_with_species = transform.apply_species_label(util.species_list, data)
    diff = df_with_species.select('species','user').subtract(expected)
    diff.show()
    assert diff.rdd.isEmpty()
