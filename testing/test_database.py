from unittest import mock

import pandas as pd
import pytest
from database_example import get_species_matches, get_hod_matches, UnknownSpeciesError

# Here are some "warm up" unit tests to get started figuring out
# the mocking pattern
@mock.patch('pandas.read_sql_query', autospec=True)
def test_species_match(mock_read_sql):
    data = [{'id': '1234', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    mock_read_sql.return_value = pd.DataFrame.from_dict(data)
    df = get_species_matches('night heron')
    assert len(df.species) > 0

@mock.patch('pandas.read_sql_query', autospec=True)
def test_bad_species(mock_read_sql):
    data = [{'id': '1234', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    mock_read_sql.return_value = pd.DataFrame.from_dict(data)
    with pytest.raises(UnknownSpeciesError):
        df = get_species_matches('!-- drop table')
        assert len(df.species) == 0

# In these solutions, Im applying the mock to the lowest level
# I have access to, the read_sql_query method in pandas. 
@mock.patch('pandas.read_sql_query', autospec=True)
def test_get_hod_matches_none(mock_read_sql):
    data_zip = [{'id': '1234', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    data_species = [{'id': '3456', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    df_zip = pd.DataFrame.from_dict(data_zip)
    df_species = pd.DataFrame.from_dict(data_species)
    mock_read_sql.side_effect = [df_species, df_zip] 
    df = get_hod_matches('night heron', '97341')
    assert len(df.id) == 0

@mock.patch('pandas.read_sql_query', autospec=True)
def test_get_hod_matches(mock_read_sql):
    data_zip = [{'id': '1234', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    data_species = [{'id': '1234', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    df_zip = pd.DataFrame.from_dict(data_zip)
    df_species = pd.DataFrame.from_dict(data_species)
    mock_read_sql.side_effect = [df_species, df_zip] 
    df = get_hod_matches('night heron', '97341')
    assert len(df.id) == 1

# This is the simple way I was thinking of where you can apply
# the mock results to the get_ methods instead of the call to
# read_sql_query. Consider what would happen if either of the get_
# methods changed - these change would not be comprehended in this
# unit test since none of the code for those methods is executed
@mock.patch('database_example.get_species_matches', autospec=True)
@mock.patch('database_example.get_zipcode_matches', autospec=True)
def test_get_hod_matches_less_coverage(mock_species, mock_zip):
    data_zip = [{'id': '1234', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    data_species = [{'id': '1234', 'content': {'count':1, 'things':'stuff'},
            'species': 'night heron'}]
    df_zip = pd.DataFrame.from_dict(data_zip)
    df_species = pd.DataFrame.from_dict(data_species)
    mock_species.return_value = df_species
    mock_zip.return_value = df_zip
    df = get_hod_matches('night heron', '97341')
    assert len(df.id) == 1

