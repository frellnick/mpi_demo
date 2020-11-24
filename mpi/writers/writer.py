# writer.py

from utils.models import MasterPersonLong, NoSQLSerializer
from utils.db import get_session, get_mongo
from config import MPIARCH

from pymongo import InsertOne, UpdateOne

writers = {}

##################
### SQL Writer ###
##################


def _write_mpi_sql(ident_inserts):
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

class MongoUpsert():
    def __init__(self):
        pass
    def __call__(self, data):
        ## TODO: Upsert Logic and figuring out where the aggregation happens.
        ## Only ever add a whole GUID, maybe not a terrible problem.
        return UpdateOne(data['mpi'], data['mpi']['sources'], upsert=True)  ## REQUIRES FIX TO ADD STUFF TO LIST


def _write_mpi_nosql(ident_inserts, upsert=False):
    Serializer = NoSQLSerializer()
    if upsert:
        operation = MongoUpsert()
    else:
        operation = MongoInsert()

    insert_objects = []
    for iarray in ident_inserts:
        insert_objects.append(operation(Serializer(iarray)))
    db = get_mongo('mpi')
    db.raw.bulk_write(insert_objects)


writers['nosql'] = _write_mpi_nosql



def write_mpi_data(ident_inserts):
    writer = writers[MPIARCH]
    writer(ident_inserts)