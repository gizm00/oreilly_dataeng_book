from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, ArrayType, IntegerType
import json

from faker_example import fake

spark = (SparkSession
         .builder
         .appName('oreilly-book')
         .getOrCreate())

# solution cribbed from https://stackoverflow.com/a/53817839

DYNAMIC_NAMED_FIELDS = {
    ("count", LongType()): lambda: fake.random.randint(0, 20),
    ("description", StringType()): lambda: fake.description(),
    ("user",StringType()): lambda: fake.email(),
}

DYNAMIC_GENERAL_FIELDS = {
    StringType(): lambda: fake.word(),
    LongType(): lambda: fake.random.randint(),
    IntegerType: lambda: fake.random.randint(),
    ArrayType(StringType()): lambda: [fake.word() for i in range(fake.random.randint(0,5))]
}

def update_expected(name, value, expected):
    if name == 'description':
        expected['species'] = value[1]
        return value[0]
    else:
        expected['user'] = value
        return value

def get_value(name, dataType, expected):
    if (name, dataType) in DYNAMIC_NAMED_FIELDS:
        value = DYNAMIC_NAMED_FIELDS.get((name, dataType))()
        if name in ['user', 'description']:
            return update_expected(name, value, expected)

        return value
    if dataType in DYNAMIC_GENERAL_FIELDS:
        return DYNAMIC_GENERAL_FIELDS.get(dataType)()

def generate_data(schema, length=1):
    gen_samples = []
    expected_values = []
    for _ in range(length):
        expected = {}
        gen_samples.append(tuple(map(lambda field: get_value(field.name, field.dataType, expected), schema.fields)))
        expected_values.append(expected)

    mock_data = spark.createDataFrame(gen_samples, schema)
    expected = spark.sparkContext.parallelize(expected_values).toDF()
    return mock_data, expected

def create_schema_and_mock_spark(filename, length=1):
    """
    If you dont have pyspark schemas you can create them
    by loading a sample of the data you want to mock and extracting
    the schema from there.
    """
    with open(filename) as f:
        sample_data = json.load(f)
    df = spark.sparkContexparallelize(sample_data).toDF()   
    return generate_data(df.schema, length)

def mock_from_schema_spark(schema, length):
    return generate_data(schema, length)


if __name__ == '__main__':
    from schemas import survey_data
    df_data, df_expected = mock_from_schema_spark(survey_data, 10)
    df_data.show(10, False)
    df_expected.show(10, False)
    