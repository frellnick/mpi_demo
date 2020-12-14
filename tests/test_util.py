# test_util_filters.py


from utils.filters import search_list
from utils.dfutils import result_proxy_to_dataframe
from db import query_db
from assets.mapping import blocked_identifiers

import pandas as pd

import pytest

from .global_test_setup import testlogger

example_cols = [
    'student_id',
    'ssn',
    'first_name',
    'usbe_student_id',
    ]




def test_search_list():
    res = []
    for col in example_cols:
        if search_list(col, blocked_identifiers):
            res.append(col)
    assert len(res) == 3


def test_result_proxy_to_dataframe():
    # TODO: build a test stable for this
    rproxy = query_db("SELECT * FROM mpi_vectors").fetchall()
    df = result_proxy_to_dataframe(rproxy) 
    assert type(df) == pd.DataFrame
