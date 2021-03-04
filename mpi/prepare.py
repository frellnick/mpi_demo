"""
prepare.py

Create indexed view of raw data and appropriate identity view for linkage.
"""

import pandas as pd
from assets.mapping import colmap, local_identifiers
from db import query_db, get_db
from utils import get_column_intersect
from config import IDVIEWTYPE
import logging

preplogger = logging.getLogger(__name__)


## Prepare identifying information for matching ##

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



def create_data_view(tablename: str) -> pd.DataFrame:
    mapped_columns = _filter_mapped_columns(tablename)
    ident_query = f"SELECT {mapped_columns} FROM {tablename}"
    raw_query = f"SELECT * FROM {tablename}"
    preplogger.debug(ident_query)
    raw = pd.read_sql_query(raw_query, get_db())
    subset = pd.read_sql_query(ident_query, get_db())  ## Deprecate for in-memory mapping
    preplogger.info(f'Data View created with \
        columns:\n{raw.columns}\nlength: {len(raw)}\nsubset contains:\n{subset.columns}'
        )
    return raw, subset




################################################
### Prepare valid identity view for matching ###
################################################

# Creating simple config registry for function assignment
registry_idview = {}


# FULL: Returns

def _build_iframe_selection_query(*args, **kwargs):
    def _add_scores_to_list(valid_columns, available_id_columns):
        for name in available_id_columns:
            if 'score' in name:
                valid_columns.append(name)
        return valid_columns

    query = "SELECT * FROM mpi_vectors LIMIT 1"
    check_selection = pd.read_sql(query, get_db())
    if 'mapped_columns' in kwargs:
        valid_columns = get_column_intersect(
            check_selection, pd.DataFrame(columns=kwargs['mapped_columns'])
            )
        valid_columns = _add_scores_to_list(valid_columns, check_selection.columns.to_list())
        valid_columns.append('mpi')
        query = f"SELECT {','.join(valid_columns)} from mpi_vectors"
    else:
        query = "SELECT * FROM mpi_vectors"
    return query



def full_id_view(*args, **kwargs) -> pd.DataFrame:
    query = _build_iframe_selection_query(*args, **kwargs)
    preplogger.debug('Preparing identity view with: \n' + query)
    
    try:
        iframe = pd.read_sql_query(query, get_db()).drop_duplicates()
    except Exception as e:
        iframe = pd.DataFrame()
        preplogger.warn(f'Could not create iframe.  Returning empty dataframe.\n{e}')

    if 'index' in iframe.columns:
        iframe.drop('index', axis=1, inplace=True)

    iframe = iframe.dropna(axis=1, how='all')
    preplogger.info(f'IFrame created with columns:\n{iframe.columns}\nlength:{len(iframe)}')
    return iframe
registry_idview['full'] = full_id_view


# Route Function - Looks up IDVIEW fn listed in settings.ini and returns view from mapped columns
def create_identity_view(*args, **kwargs) -> pd.DataFrame:
    preplogger.info(f'Using IDVIEWTYPE: {IDVIEWTYPE}')
    fn = registry_idview[IDVIEWTYPE]
    return fn(*args, **kwargs)

