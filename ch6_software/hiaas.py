# Heron Identification as a Service

def run_extract_species(customer_data, database_config):
    extracted = extract_species(customer_data)
    store_data(extracted, database_config)


def run_extract_species(bucket, db, customer_id):
    raw = get_data(bucket)
    extracted = extract_species(raw)
    store_data(extracted, db, customer_id)


from hiaas_config import configs
for config in configs:
    run_extract_species(**config)