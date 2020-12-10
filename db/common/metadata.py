# metadata.py

from db import query_db


def get_table_columns(tablename: str) -> list:
    query = \
        f"""
        SELECT m.name, p.name as columnName 
        FROM sqlite_master m LEFT OUTER JOIN pragma_table_info((m.name)) p
            ON m.name <> p.name
        WHERE m.name = '{tablename}';
        """
    try:
        res = query_db(query).fetchall()
        cols = [r.columnName for r in res]
        err = None
    except Exception as e:
        cols = None
        err = e
    return cols, err