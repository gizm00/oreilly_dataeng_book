
from pyspark.sql import Row
from collections import OrderedDict

species_list = ["night heron", "great blue heron", "grey heron", "whistling heron"]

def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))

def df_from_list_dict(data, sc):
    return (sc.parallelize(data)
        .map(convert_to_row)
        .toDF())