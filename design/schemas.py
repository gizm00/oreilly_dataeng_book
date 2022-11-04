
from pyspark.sql.types import StructType, LongType, StringType, ArrayType, StructField


StructType(
    [StructField("count", LongType(),True), 
    StructField("description", StringType(),True),
    StructField("user", StringType(),False),
    StructField("img_files", ArrayType(StringType(),True),True),
    StructField("location", ArrayType(StringType(),True),True)]
)