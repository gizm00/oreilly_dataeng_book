import pandas as pd
from sqlalchemy import create_engine

from util import species_list

# If you arent familiar with the sqlalchemy ORM this is a really nice article:
# https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91
# The example methods below are making direct sql queries for the sake of
# illustration. In a production environment you would want to consider using an ORM 

engine = create_engine('postgresql://user:password@localhost:5432/hod_db')

class UnknownSpeciesError(BaseException):
    pass

def check_species(species):
    return species in species_list

def get_species_matches(species):
    if not check_species(species):
        raise UnknownSpeciesError
    query = f"select id, content from hod_user where species = '{species}'"
    return pd.read_sql_query(query, con=engine)

def get_zipcode_matches(zipcode):
    query = f"select id, content from hod_user where zipcode = '{zipcode}'"
    return pd.read_sql_query(query, con=engine)

def get_hod_matches(species, zipcode):
    """
    This function could be a single SQL query but Ive split it
    for the purposes of mocking illustration
    """
    df_species = get_species_matches(species)
    df_zipcode = get_zipcode_matches(zipcode)
    return df_species.merge(df_zipcode, how='inner', on='id')