"""
prepare.py

Create indexed view of raw data and appropriate identity view for linkage.
"""

import pandas as pd
from assets.mapping import colmap, local_identifiers
from .standardize import standardize
from .view import View
from db import query_db, get_db
from db.common import get_table_columns
from utils import Registry
from config import IDVIEWTYPE
import logging

preplogger = logging.getLogger(__name__)


# Creating simple config registry for function assignment
registry_idview = {}
view_registry = Registry(name='views')

## Prepare identifying information for matching ##
def create_data_view(tablename: str) -> pd.DataFrame:
    raw_query = f"SELECT * FROM {tablename}"
    dview = View(pd.read_sql_query(raw_query, get_db()), context={'partner': tablename.split('_')[0]})

    preplogger.info(f'Data View created with statement:\n{raw_query}')
    preplogger.info(f'Data View created with columns:\n{dview.columns}\nlength: {len(dview)}')

    return dview



################################################
### Prepare valid identity view for matching ###
################################################


# FULL: Returns

def _build_iframe_selection_query(*args, **kwargs):
    assert 'mapped_columns' in kwargs, "Must supply kwarg mapped_columns: list of columns to make iview from."
    def _get_score_columns(columns):
        t = []
        for name in columns:
            if 'score' in name:
                t.append(name)
        return t

    mpi_columns, _ = get_table_columns('mpi_vectors')
    iframe_columns = get_intersection(mpi_columns, kwargs['mapped_columns'])
    [iframe_columns.add(x) for x in _get_score_columns(mpi_columns)]
    iframe_columns.add('mpi')

    return f"SELECT {','.join(iframe_columns)} from mpi_vectors"



def full(*args, **kwargs) -> pd.DataFrame:
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
    preplogger.info(f'IView created with columns:\n{iframe.columns}\nlength:{len(iframe)}')
    return View(iframe)
view_registry.register(full)


# Route Function - Looks up IDVIEW fn listed in settings.ini and returns view from mapped columns
def create_identity_view(*args, **kwargs) -> pd.DataFrame:
    preplogger.info(f'Using IDVIEWTYPE: {IDVIEWTYPE}')
    fn = view_registry[IDVIEWTYPE]
    return fn(*args, **kwargs)




## Utility Functions
def get_intersection(i1, i2) -> set:
    def _cast_set(x) -> set:
        if type(x) == set:
            return x 
        return set(x)
    
    a = _cast_set(i1)
    b = _cast_set(i2)
    return a.intersection(b)