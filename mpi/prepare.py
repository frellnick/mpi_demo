"""
prepare.py

Create indexed view of raw data and appropriate identity view for linkage.
"""

import pandas as pd
from assets.mapping import colmap, local_identifiers
from utils.db import query_db, get_db
import logging

logger = logging.getLogger(__name__)


## Prepare identifying information for matching ##

def create_distinct_view(tablename: str) -> pd.DataFrame:
    mapped_columns = _filter_mapped_columns(tablename)
    query = f"SELECT {mapped_columns} FROM {tablename}"
    logging.debug(query)
    return pd.read_sql_query(query, get_db()).drop_duplicates()


def _filter_mapped_columns(tablename: str) -> str:
    def _rename_column(col):
        pool_name = colmap[col]  # get standard name from mapping
        if pool_name in local_identifiers:
            return ' '.join([col, 'AS', tablename.split('_')[0] + '_' + pool_name]).lower()
        return ' '.join([col, 'AS', pool_name]).lower()

    table_columns = query_db(
        f"SELECT * FROM {tablename}"
    ).first().keys()
    logging.debug(f'Querying for column names of {tablename}. Returned: {table_columns}')
    mapped_cols = [_rename_column(col) for col in table_columns if col in colmap.keys()]
    return ','.join(mapped_cols)


## Prepare valid identity view for matching ##

def create_identity_view(mapped_columns: list) -> pd.DataFrame:
    fields = ','.join(["'"+col+"'" for col in mapped_columns])
    query_long = f"SELECT * FROM master_person_long WHERE field in ({fields})"
    logging.debug(query_long)
    view_long = query_db(query_long)
    return view_long.fetchall()