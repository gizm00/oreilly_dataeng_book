from unittest import mock

import pandas as pd
import pytest
from database_example import get_species_matches, get_hod_matches, UnknownSpeciesError

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

