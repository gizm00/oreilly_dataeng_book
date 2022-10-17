from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, ArrayType, IntegerType
import json

from faker_example import fake

spark = (SparkSession
         .builder
         .appName('oreilly-book')
         .getOrCreate())

class DataGenerationError(BaseException):
    pass

# solution cribbed from https://stackoverflow.com/a/53817839

DYNAMIC_NAMED_FIELDS = {
    ("count", LongType()): lambda: fake.random.randint(0, 20),
    ("description", StringType()): lambda: fake.description_distribution(),
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
    if name == 'user':
        expected['user'] = value
        return value
    raise DataGenerationError(f"Could not update expected value for name {name}")

def get_value(name, datatype):
    if name in [t[0] for t in DYNAMIC_NAMED_FIELDS]:
        value = DYNAMIC_NAMED_FIELDS[(name, datatype)]()
        return value

    if datatype in DYNAMIC_GENERAL_FIELDS:
        return DYNAMIC_GENERAL_FIELDS.get(datatype)()
        
    raise DataGenerationError(f"No match for {name}, {datatype}") 

def generate_data(schema, length=1):
    gen_samples = []
    for _ in range(length):
        gen_samples.append(tuple(map(lambda field: get_value(field.name, field.dataType), schema.fields)))

    return spark.createDataFrame(gen_samples, schema)

### Updated to provide expected results

DYNAMIC_NAMED_FIELDS_EXPECTED = {
    ("count", LongType()): lambda: fake.random.randint(0, 20),
    ("description", StringType()): lambda: fake.description_distribution_expected(),
    ("user",StringType()): lambda: fake.email(),
}

def get_value_update(name, datatype, expected):
    if name in [t[0] for t in DYNAMIC_NAMED_FIELDS_EXPECTED]:
        value = DYNAMIC_NAMED_FIELDS_EXPECTED[(name, datatype)]()

        if name in ['user', 'description']:
            return update_expected(name, value, expected)
        return value

    if datatype in DYNAMIC_GENERAL_FIELDS:
        return DYNAMIC_GENERAL_FIELDS.get(datatype)()

    raise DataGenerationError(f"No match for {name}, {datatype}")

def generate_data_expected(schema, length=1):
    gen_samples = []
    expected_values = []
    for _ in range(length):
        expected = {}
        gen_samples.append(tuple(map(lambda field: get_value_update(field.name, field.dataType, expected), schema.fields)))
        expected_values.append(expected)

    mock_data = spark.createDataFrame(gen_samples, schema)
    expected = expected_values
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
    