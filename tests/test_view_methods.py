# test_view_methods.py

from mpi.prepare import View
from ingest import load_file
import pandas as pd

from db import get_db

import pytest


@pytest.fixture
def test_frame():
    load_file('assets/data/dws_wages.csv', 'dws_wages')
    raw_query = f"SELECT * FROM dws_wages"
    raw = pd.read_sql_query(raw_query, get_db())
    return raw


def test_view_subset(test_frame):
    v = View(test_frame, context={'partner':'dws'})
    assert len(v.subset.columns) < len(v.columns)


def test_view_merge(test_frame):
    v = View(test_frame, context={'partner':'dws'})
    mpi_vec = list(range(0, len(v.subset)))
    id_frame = v.subset.copy()
    id_frame['mpi'] = mpi_vec
    combined = v.merge(id_frame)
    assert len(combined) > 0