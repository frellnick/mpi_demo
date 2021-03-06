# dataframewriter.py

from db import get_db

def dataframe_to_db(dataframe, tablename='temp', index=False, if_exists='replace'):
    db = get_db()
    dataframe.to_sql(
        name=tablename, 
        con=db.connection,
        if_exists=if_exists,
        index=index,
        )
    return tablename