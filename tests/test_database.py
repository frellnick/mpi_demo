# test_database


from utils.db import init_db, get_db, get_mongo
from utils.models import validate_model
import pytest

def test_init_db():
    init_db()


def test_get_db():
    db = get_db()
    assert(db is not None)
    

def test_mongo_client():
    testdb = get_mongo()
    assert testdb is not None


def test_write_destroy_mongodb():
    test = get_mongo('test')
    collection = test['temp']

    inserted_id = collection.insert_one({'test': 10}).inserted_id
    assert inserted_id is not None 

    x = collection.delete_many({})
    assert x.deleted_count == 1



def test_model_validation():

    good = {'mpi': '001', 'sources': [{'guid': 111, 'fields': [{'fieldname': 'field1', 'value': 100}]}]}
    bad = {'mpi': '001', 'sources': [{'guid': None, 'fields': [{'fieldname': 'field1', 'value': 100}]}]}

    assert validate_model(data=good) == True 
    assert validate_model(data=bad) == False