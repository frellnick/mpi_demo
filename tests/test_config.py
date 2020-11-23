#test_config.py

import pytest

@pytest.fixture
def config():
    import config as c
    return c


def test_config(config):
    assert (config.DEBUG == True) or (config.DEBUG == False)

    dbconfig = config.DATABASES
    assert 'default' in dbconfig.keys()
    assert 'nosql' in dbconfig.keys()
