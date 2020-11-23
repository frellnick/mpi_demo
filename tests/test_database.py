# test_database


from utils.db import init_db, get_db


def test_init_db():
    init_db()


def test_get_db():
    db = get_db()
    assert(db is not None)
    