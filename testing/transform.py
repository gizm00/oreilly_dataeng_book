from pyspark.sql import functions as f


def apply_species_label(species_list, df):
    species_regex = f".*({'|'.join(species_list)}).*"
    return (df
        .withColumn("description_lower", f.lower('description'))
        .withColumn("species", f.regexp_extract('description_lower', species_regex, 1))
        .drop("description_lower")
    )
