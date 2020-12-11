# test_flags.py


import pytest
from main import run_mpi
from db import get_session, get_mongo
from db.common import clear_databases


from mpi.postprocess import Flag


@pytest.fixture
def tables():
    tables = ['usbe_students', 'dws_wages']
    # Setup
    for t in tables:
        run_mpi(tablename=t)
    yield tables
    # Teardown
    clear_databases()



def test_create_flag():
    f = Flag()


def test_run_flag():
    pass