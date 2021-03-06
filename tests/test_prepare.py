# test_prepare.py

import pytest

import pandas as pd

from ingest import load_file
from mpi.prepare import create_data_view, create_identity_view, standardize
from mpi.prepare.view import View
from db import get_db

from assets.mapping import colmap

from .global_test_setup import testlogger



@pytest.fixture
def test_table():
    load_file('assets/data/dws_wages.csv', 'dws_wages')
    return 'dws_wages'


@pytest.fixture
def test_frame():
    load_file('assets/data/dws_wages.csv', 'dws_wages')
    raw_query = f"SELECT * FROM dws_wages"
    raw = pd.read_sql_query(raw_query, get_db())
    return raw


def test_create_data_view(test_table):
    testlogger.info(f'Starting Create Data View: Loading {test_table}', __name__)
    dview = create_data_view(test_table)
    assert dview is not None
    assert len(dview.columns) > 0


def test_create_identity_view(test_table):
    dview = create_data_view(test_table)
    iview = create_identity_view(mapped_columns=dview.subset.columns)
    assert iview is not None
    for col in iview.columns:
        assert col=='mpi' or (col in colmap.values()), f'{col} not valid mapped column'


def test_dataframe_view(test_frame):
    view = View(test_frame)
    assert len(view) > 0
    assert len(view.columns) > 0
    assert view[view.columns[0]] is not None


def test_standardize_data_view(test_frame):
    dview = test_frame
    testlogger.debug(f"Dview Pre:\n{dview.head()}")
    dview = standardize(dview)
    testlogger.debug(f"Dview Post:\n{dview.head()}")
    assert len(dview) > 0

