# writer.py

from utils.models import MasterPersonLong, NoSQLSerializer
from utils.db import get_session, get_mongo
from config import MPIARCH

from pymongo import InsertOne, UpdateOne


writers = {}


def write_mpi_data(ident_inserts, dbtype=None, update=False):
    if dbtype is not None:
        writer = writers[dbtype]
    else:
        writer = writers[MPIARCH]
    writer(ident_inserts, update)


##################
### SQL Writer ###
##################


def _write_mpi_sql(ident_inserts, update=False):
    insert_objects = []
    for iarray in ident_inserts:
        insert_objects.extend([MasterPersonLong(**kwargs) for kwargs in iarray])
    with get_session() as session:
        session.bulk_save_objects(insert_objects)
        session.flush()
        session.commit()
writers['sql'] = _write_mpi_sql



####################
### NoSQL Writer ###
####################

class MongoInsert():
    def __init__(self):
        pass
    def __call__(self, data):
        return InsertOne(data)

class MongoUpdate():
    def __init__(self):
        pass
    def __call__(self, data):
        ## TODO: Update Logic and figuring out where the aggregation happens.
        ## Only ever add a whole GUID, maybe not a terrible problem.
        return UpdateOne(
            {'mpi': data['mpi']}, 
            {"$addToSet": {'sources': data['sources'][0]}},
            )


def _write_mpi_nosql(ident_inserts, update=False):
    def _check_index_exists(name):
        for ind in db.raw.list_indexes():
            if name in ind['name']:
                return True
        return False

    Serializer = NoSQLSerializer()
    if update:
        operation = MongoUpdate()
    else:
        operation = MongoInsert()

    insert_objects = []
    for iarray in ident_inserts:
        insert_objects.append(operation(Serializer(iarray)))
    db = get_mongo('mpi')
    db.raw.bulk_write(insert_objects)

    # Create MPI index if not done already
    if not _check_index_exists('mpi'):
        db.raw.create_index('mpi', unique=True)
writers['nosql'] = _write_mpi_nosql