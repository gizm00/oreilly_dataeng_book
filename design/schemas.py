
from pyspark.sql.types import StructType, LongType, StringType, ArrayType, StructField


spark_schema = StructType(
    [StructField("count", LongType(),True), 
    StructField("description", StringType(),True),
    StructField("user", StringType(),False),
    StructField("img_files", ArrayType(StringType(),True),True),
    StructField("location", ArrayType(StringType(),True),True)]
)

initial_json_schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "array",
    "items": [{
        "type": "object",
        "properties": {
        "user": {"type": "string"},
        "location": {
            "type": "array",
            "items": [
                {"type": "string"},
                {"type": "string"}
            ]
        },
        "img_files": {
            "type": "array",
            "items": [{"type": "string"}]
        },
        "description": {
            "type": "string"
        },
        "count": {
            "type": "integer"
        }
        },
        "required": [
        "user",
        "location"
        ]
    }]
}

updated_schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "array",
    "items": [{
        "type": "object",
        "properties": {
        "user": {"type": "string"},
        "location": {
            "type": "array",
            "minItems":2,
            "items": [
                {"type": "string"}
            ]
        },
        "img_files": {
            "type": "array",
            "items": [{"type": "string"}]
        },
        "description": {
            "type": "string"
        },
        "count": {
            "type": "integer"
        }
        },
        "required": [
        "user",
        "location"
        ]
    }]
}