from pyspark.sql import SparkSession
from pyspark.sql import types as T
import json

from faker_example import fake

spark = (SparkSession
         .builder
         .appName('oreilly-book')
         .getOrCreate())

# solution cribbed from https://stackoverflow.com/a/53817839

DYNAMIC_NAMED_FIELDS = {
    ("count", T.LongType()): lambda: fake.random.randint(0, 20),
    ("description", T.StringType()): lambda: fake.description_only(),
    ("user",T.StringType()): lambda: fake.email(),
}

DYNAMIC_GENERAL_FIELDS = {
    T.StringType(): lambda: fake.word(),
    T.LongType(): lambda: fake.random.randint(),
    T.IntegerType: lambda: fake.random.randint(),
    T.ArrayType(T.StringType()): lambda: [fake.word() for i in range(fake.random.randint(0,5))]
}

def get_value(name, dataType):
    # If using regular description with both species and description, we could
    # add a global 1 element dict to hold the value of the description, user, and the
    # species which could be referenced based on field name
    if (name, dataType) in DYNAMIC_NAMED_FIELDS:
        return DYNAMIC_NAMED_FIELDS.get((name, dataType))()
    if dataType in DYNAMIC_GENERAL_FIELDS:
        return DYNAMIC_GENERAL_FIELDS.get(dataType)()

def generate_data(schema, length=1):
    gen_samples = []
    for _ in range(length):
        gen_samples.append(tuple(map(lambda field: get_value(field.name, field.dataType), schema.fields)))

    return spark.createDataFrame(gen_samples, schema)

def create_schema_and_mock_spark(filename, length=1):
    """
    If you dont have pyspark schemas you can create them
    by loading a sample of the data you want to mock and extracting
    the schema from there.
    """
    with open(filename) as f:
        fake_data = json.load(f)
    df = spark.sparkContext.parallelize(fake_data).toDF()   
    return generate_data(df.schema, length)

def mock_from_schema_spark(schema, length):
    return generate_data(schema, length)


if __name__ == '__main__':
    from schemas import survey_data
    df = mock_from_schema_spark(survey_data, 10)
    df.show()
    
