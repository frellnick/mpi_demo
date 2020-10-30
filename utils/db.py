"""
Database
    Initialize and create connection control flow for database.
    Datase parameters must be set in config.py or directly in app.py
"""


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from utils.models import *
from config import DATABASES

import logging

from app_global import g


def get_db():
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


## DB Utils ##

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

    