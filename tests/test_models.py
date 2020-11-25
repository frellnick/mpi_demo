# test_models.py


from utils.db import get_db
from utils.models import NoSQLSerializer
from utils.generators import gen_mpi_insert

from .global_test_setup import testlogger

import pandas as pd

import pytest 


@pytest.fixture
def test_data():
    query = f"SELECT * FROM mpi_vectors_test"
    return gen_mpi_insert(
        pd.read_sql_query(query, get_db())
    )


def test_nosqlserializer(test_data):
    ser = NoSQLSerializer()
    out = []
    for row in test_data:
        out.append(ser(row))
    testlogger.debug(out, __name__)
    for x in out:
        assert x is not None
