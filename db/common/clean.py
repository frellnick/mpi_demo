# clean.py

from db import get_mongo, get_session

def clear_databases():
    mg = get_mongo()
    mg.raw.delete_many({})
    
    with get_session() as session:
        session.execute(
            'DELETE FROM mpi_vectors WHERE 1=1;'
        )
        session.commit()