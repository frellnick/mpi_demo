# test_database


from utils.db import init_db, get_db, get_mongo


def test_init_db():
    init_db()


def test_get_db():
    db = get_db()
    assert(db is not None)
    

def test_mongo_client():
    testdb = get_mongo()
    assert testdb is not None


def test_write_destroy_mongodb():
    mdb = get_mongo()
    test = mdb.test
    collection = test['temp']

    inserted_id = collection.insert_one({'test': 10}).inserted_id
    assert inserted_id is not None 

    x = collection.delete_many({})
    assert x.deleted_count == 1
