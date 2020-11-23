#test_config.py

import pytest

@pytest.fixture
def config():
    import config as c
    return c


def test_config(config):
    assert (config.DEBUG == True) or (config.DEBUG == False)

    dbconfig = config.DATABASES
    assert SQL_URI in dbconfig.keys()
    assert NOSQL_URI in dbconfig.keys()
