from pyspark.sql import types as T

survey_data = T.StructType(
    [T.StructField("count",T.LongType(),True), 
    T.StructField("description",T.StringType(),True),
    T.StructField("user",T.StringType(),True),
    T.StructField("img_files",T.ArrayType(T.StringType(),True),True),
    T.StructField("location",T.ArrayType(T.StringType(),True),True)]
)