# test_update.py

from mpi.writers import write_mpi_data, writers
import pytest 

@pytest.fixture
def config():
    import config as c
    return c


def test_mpi_writer_config(config):
    assert writers[config.MPIARCH] is not None