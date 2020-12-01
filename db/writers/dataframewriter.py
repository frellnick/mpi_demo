# dataframewriter.py

from db import get_db

def dataframe_to_db(dataframe, tablename='temp', index=False):
    db = get_db()
    dataframe.to_sql(
        name=tablename, 
        con=db.connection,
        if_exists='replace',
        index=index,
        )
    return tablename