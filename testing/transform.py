from pyspark.sql import functions as f


def apply_species_label(species_list, df):
    species_regex = f".*({'|'.join(species_list)}).*"
    return (df
        .withColumn("description_lower", f.lower('description'))
        .withColumn("species", f.regexp_extract('description_lower', species_regex, 1))
        .drop("description_lower")
    )


def faster_species_label(species_dict, df):
    """
    species_dict:
    {
        'heron': ['great blue heron', 'night heron'],
        'jay': ['grey', 'stellar', 'blue']
    }
    
    Instead of searching the entire list, match to species first and type after
    """
    return (df
        .withColumn("description_lower", f.lower('description'))
        .withColumn("species_key", f.regexp_extract('description_lower', '(?=^|\s)(' + '|'.join(species_dict.keys()) + ')(?=\s|$)', 0))
        .withColumn("species", f.column('description_lower', '(?=^|\s)(' + '|'.join(species_dict[df.species_key]) + ')(?=\s|$)', 0))
        .removeColumn("description_lower")
        .removeColumn("species_key")
    )