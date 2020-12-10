# test_database


from db import init_db, get_db, get_mongo
from db import validate_model
from db.common import get_table_columns, clear_databases
import pytest

from .global_test_setup import testlogger

def test_init_db():
    init_db()


def test_get_db():
    db = get_db()
    assert(db is not None)
    

def test_mongo_client():
    testdb = get_mongo()
    assert testdb is not None


@pytest.fixture
def setup():
    yield True
    clear_databases()


def test_write_destroy_mongodb(setup):
    test = get_mongo('test')
    collection = test['temp']

    inserted_id = collection.insert_one({'test': 10}).inserted_id
    assert inserted_id is not None 

    x = collection.delete_many({})
    assert x.deleted_count == 1


def test_model_validation(setup):

    good = {'mpi': '001', 'sources': [{'guid': 111, 'score': 0.99, 'fields': [{'fieldname': 'field1', 'value': 100}]}]}
    bad = {'mpi': '001', 'sources': [{'guid': None, 'score': 1.00, 'fields': [{'fieldname': 'field1', 'value': 100}]}]}

    assert validate_model(data=good) == True 
    assert validate_model(data=bad) == False


def test_common_get_table_columns(setup):
    testlogger.info('Start', __name__)
    cols, err = get_table_columns('mpi_vectors')
    assert err is None
    if err is None:
        assert type(cols) == list
        testlogger.debug(f'{cols}', __name__)
    else:
        testlogger.error(f'err')
        raise ValueError(f'{err}')
    
    