# test_flags.py


import pytest
from main import run_mpi
from db import get_session, get_mongo
from db.common import clear_databases


from mpi.postprocess import Rule1, Rule2

from .global_test_setup import testlogger


@pytest.fixture
def tables():
    tables = ['usbe_students', 'dws_wages']
    # Setup
    for t in tables:
        run_mpi(tablename=t)
    yield tables
    # Teardown
    clear_databases()



def test_rule_flags(tables):
    testlogger.info("Starting flag test: Rule2", __name__)
    flag1 = Rule1
    rep = flag1()
    assert rep is not None

    flag2 = Rule2
    rep = flag2()
    assert rep is not None