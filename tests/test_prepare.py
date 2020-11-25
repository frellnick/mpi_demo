# test_prepare.py

import pytest
from ingest import load_file
from mpi.prepare import create_data_view, create_identity_view

from .global_test_setup import testlogger


@pytest.fixture
def test_table():
    load_file('assets/data/dws_wages.csv', 'dws_wages')
    return 'dws_wages'

def test_create_data_view(test_table):
    testlogger.info(f'Starting Create Data View: Loading {test_table}', __name__)
    raw, dview = create_data_view(test_table)
    assert raw is not None
    assert dview is not None
    assert len(raw.columns) >= len(dview.columns)
    assert len(raw) >= len(dview)


def test_create_identity_view(test_table):
    iview = create_identity_view()
    assert iview is not None