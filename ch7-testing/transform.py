

def apply_species_label(species_list, df):
    return df.withColumn("species", species_lookup())