# test_flags.py


import pytest
from main import run_mpi
from db import get_session, get_mongo

from mpi.postprocess import Flag


def cleanup_and_report(name:str=None) -> dict:
    report = {}
    report['name'] = name
    
    mg = get_mongo()
    res = mg.raw.delete_many({})
    mg_deleted = res.deleted_count
    report['count_documents'] = mg_deleted
    
    with get_session() as session:
        count_vectors = session.execute(
            'SELECT COUNT(*) FROM mpi_vectors'
        ).fetchone()
        session.execute(
            'DELETE FROM mpi_vectors WHERE 1=1;'
        )
        session.commit()
    report['count_vectors'] = count_vectors.values()[0]
    
    return report


@pytest.fixture
def tables():
    tables = ['usbe_students', 'dws_wages']
    # Setup
    for t in tables:
        run_mpi(tablename=t)
    yield tables
    # Teardown
    cleanup_and_report(name='Test')




def test_create_flag():
    f = Flag()


def test_run_flag():
    pass