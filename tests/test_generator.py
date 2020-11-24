# test_generator.py


import pytest
import pandas as pd
import json

from utils.db import dataframe_to_db, get_db

from utils.generators import gen_mpi_insert

from .global_test_setup import testlogger

mpi_vectors = [
    {'mpi': 1, 'last_name': None, 'first_name': 'elvis', 'guid': 11},
    {'mpi': 1, 'last_name': 'presley', 'first_name': 'elvis', 'guid': 12},
    {'mpi': 1, 'last_name': 'costello', 'first_name': 'elvis', 'guid': 13},
    {'mpi': 2, 'last_name': 'austin', 'first_name': 'jane', 'guid': 11},
    {'mpi': 2, 'last_name': 'austin', 'first_name': 'janet', 'guid': 12},
]

@pytest.fixture
def test_data(mpi_vectors=mpi_vectors):
    raw_df = pd.DataFrame.from_records(mpi_vectors)
    dataframe_to_db(raw_df, 'mpi_vectors_test')
    query = f"SELECT * FROM mpi_vectors_test"
    return pd.read_sql_query(query, get_db())


def test_mpi_insert(test_data):
    inserts = gen_mpi_insert(test_data)
    for i in inserts:
        testlogger.info(json.dumps(i))