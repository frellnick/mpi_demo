# writer.py

from utils.models import MasterPersonLong
from utils.db import get_session
from config import MPIARCH



writers = {}

def _write_mpi_sql(ident_inserts):
    insert_objects = []
    for iarray in ident_inserts:
        insert_objects.extend([MasterPersonLong(**kwargs) for kwargs in iarray])
    with get_session() as session:
        session.bulk_save_objects(insert_objects)
        session.flush()
        session.commit()
writers['sql'] = _write_mpi_sql



def _write_mpi_nosql(insert_objects):
    pass
writers['nosql'] = _write_mpi_nosql



def write_mpi_data(ident_inserts):
    writer = writers[MPIARCH]
    writer(ident_inserts)