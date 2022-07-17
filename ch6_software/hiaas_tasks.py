

def acquire_data(context):
    """
    Given a bucket name, extract data and return
    for further processing
    """
    source_bucket = context["params"].get(source_bucket)
    pass


def extract_species_ml_query(data_location):
    """
    Given a data location, load the data
    and run the fancy algorithm to identify
    the species. Return the data with the
    species label attached
    """
    pass

def extract_species(data_location):
    """
    Given a data location, load the data
    and run the fancy algorithm to identify
    the species. Return the data with the
    species label attached
    """
    pass


def save_to_db(extract_location, db, customer_id):
    """
    Read the data from the extract_location, lookup
    the credentails with the customer_id, and create
    the database connection string by looking up the db
    Save the extracted data to the database
    """