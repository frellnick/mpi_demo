"""
prepare.py

Create indexed view of raw data and appropriate identity view for linkage.
"""

import pandas as pd
from assets.mapping import colmap, local_identifiers
from utils.db import query_db, get_db
from config import IDVIEWTYPE
import logging

preplogger = logging.getLogger(__name__)


## Prepare identifying information for matching ##

def create_data_view(tablename: str) -> pd.DataFrame:
    mapped_columns = _filter_mapped_columns(tablename)
    ident_query = f"SELECT {mapped_columns} FROM {tablename}"
    raw_query = f"SELECT * FROM {tablename}"
    preplogger.debug(ident_query)
    raw = pd.read_sql_query(raw_query, get_db())
    subset = pd.read_sql_query(ident_query, get_db())  ## Deprecate for in-memory mapping
    return raw, subset


def _filter_mapped_columns(tablename: str) -> str:
    def _rename_column(col):
        pool_name = colmap[col]  # get standard name from mapping
        if pool_name in local_identifiers:
            return ' '.join([col, 'AS', tablename.split('_')[0] + '_' + pool_name]).lower()
        return ' '.join([col, 'AS', pool_name]).lower()

    table_columns = query_db(
        f"SELECT * FROM {tablename}"
    ).first().keys()
    preplogger.debug(f'Querying for column names of {tablename}. Returned: {table_columns}')
    mapped_cols = [_rename_column(col) for col in table_columns if col in colmap.keys()]
    return ','.join(mapped_cols)


################################################
### Prepare valid identity view for matching ###
################################################

# Creating simple config registry for function assignment
registry_idview = {}


# FULL: Returns 
def full_id_view(*args, **kwargs) -> pd.DataFrame:
    query = "SELECT * FROM mpi_vectors"
    if 'mapped_columns' in kwargs:
        query = f"SELECT {','.join(kwargs['mapped_columns'])} from mpi_vectors"
    preplogger.debug('Preparing identity view with: \n' + query)
    
    try:
        iframe = pd.read_sql_query(query, get_db()).drop_duplicates()
    except:
        iframe = pd.DataFrame()
        preplogger.warn('Could not create iframe.  Returning empty dataframe.')

    if 'index' in iframe.columns:
        iframe.drop('index', axis=1, inplace=True)
    return iframe
registry_idview['full'] = full_id_view


# Route Function - Looks up IDVIEW fn listed in settings.ini and returns view from mapped columns
def create_identity_view(*args, **kwargs) -> pd.DataFrame:
    preplogger.info(f'Using IDVIEWTYPE: {IDVIEWTYPE}')
    fn = registry_idview[IDVIEWTYPE]
    return fn(*args, **kwargs)

