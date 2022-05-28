from pyspark.sql import SparkSession
from pyspark.sql import types as T
import json

from faker_example import fake

spark = (SparkSession
         .builder
         .appName('oreilly-book')
         .getOrCreate())


DYNAMIC_FIELDS = {
    ("count", T.LongType()): lambda: fake.random.randint(0, 20),
    ("description", T.StringType()): lambda: fake.description_only(),
    ("user",T.StringType()): lambda: fake.email(),
}

def get_value(name, dataType):
    ret = DYNAMIC_FIELDS.get((name, dataType))
    if ret:
        return ret()

def generate_data(schema, length=1):
    gen_samples = []
    for _ in range(length):
        gen_samples.append(tuple(map(lambda field: get_value(field.name, field.dataType), schema.fields)))

    return spark.createDataFrame(gen_samples, schema)

if __name__ == '__main__':
    with open('fake_survey_data.json') as f:
        fake_data = json.load(f)
    df = spark.sparkContext.parallelize(fake_data).toDF()   
    mock = generate_data(df.schema, 20)
    mock.show()