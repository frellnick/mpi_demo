"""
Database
    Initialize and create connection control flow for database.
    Datase parameters must be set in config.py or directly in app.py
"""

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from utils.models import *
from config import DATABASES

import logging

from app_global import g

###############################
### SQL Database Connection ###
###############################
def get_db() -> sqlalchemy.engine:
    """
    Returns current database connection.  If connection not present,
    initiates connection to configured database.  Default is non-authenticated SQL.
    Modifty g.db = *connect to match intended database connection.
    """
    db_uri = DATABASES['default']

    db_logger = logging.getLogger(__name__ + '.getdb')
    if not hasattr(g, 'db'):
        db_logger.info('DB connection not found. Attempting connection to {}.'.format(db_uri))
        try:
            g.engine = create_engine(db_uri)
            g.db = g.engine.connect()
        except:
            db_logger.error('Could not establish connection.  Aborting.')
            raise ConnectionError

    return g.db


@contextmanager
def get_session():
    # Setup session with thread engine.
    #   Allows for usage: with get_session() as session: session...
    engine = get_db()
    session = scoped_session(sessionmaker(bind=engine))
    try:
        yield session
    finally:
        session.close()


def close_db(e=None):
    db = get_session()
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    Base.metadata.create_all(db)


## SQL DB Utils ##

def query_db(query):
    db = get_db()
    return db.execute(query)


def dataframe_to_db(dataframe, tablename='temp'):
    db = get_db()
    dataframe.to_sql(
        name=tablename, 
        con=db.connection,
        if_exists='replace')
    return tablename


######################
### NoSQL Mongo DB ###
######################

from pymongo import MongoClient

def get_mongo(dbname='mpi') -> MongoClient:
    """
    Creates MongoDB connection connects to database specified in dbname.
    Parameters:
        dbname (str): name of database to connect to
    """
    db_uri = DATABASES['nosql']

    db_logger = logging.getLogger(__name__ + '.getdb')
    if not hasattr(g, 'mongoclient'):
        db_logger.info('MongoDB connection not found. Attempting connection to {}.'.format(db_uri))
        try:
            g.mongoclient = MongoClient(db_uri)
        except:
            db_logger.error('Could not establish connection.  Aborting.')
            raise ConnectionError

    return g.mongoclient[dbname]


def init_mongo():
    db = get_mongo()
    # Create raw collection and MPI index
    db.mpi.raw.create_index('mpi', {'unique': True})



## NoSQL DB utils
def yield_mpi_document_batch(chunk_size=10):
    """
    Generator to yield chunks from MongoDB
    :param chunk_size: int, number of records to return per chunk
    :return: list
    """
    
    chunk = []
    coll = get_mongo().raw
    cursor = coll.find({}, batch_size=chunk_size)
    
    for i, doc in enumerate(cursor):
        if (i % chunk_size == 0) and (i > 0):
            yield chunk
            del chunk[:]
        chunk.append(doc)
    yield chunk

