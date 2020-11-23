#test_config.py

import pytest

@pytest.fixture
def config():
    import config as c
    return c


def test_config(config):
    assert config.DATABASES is not None 

    assert (config.DEBUG == True) or (config.DEBUG == False)
