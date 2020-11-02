"""
link.py

linking workflow.  checks prepared records and runs through link sequence similar to:

blocking, indexing, comparing, classifying
"""

import pandas as pd
from utils.generators import generate_random_mpi, gen_mpi_insert
from utils.db import get_session
from utils.models import MasterPersonLong

from recordlinkage.preprocessing import clean

from assets.mapping import blocked_identifiers, sneighbourhood_identifiers
from recordlinkage import Index
from recordlinkage.index import Block, SortedNeighbourhood

from recordlinkage import Compare
from pandas.api.types import is_numeric_dtype

import logging

linklogger = logging.getLogger(__name__)



def _match(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    """ Block, Index, Compare, Classify records across both frames """
    d1 = t1.copy()
    if len(t2) == 0:
        d1['mpi'] = None
    else:
        d1['mpi'] = _lookup_existing(t1, t2)
    return d1[d1.mpi.notna()], d1[d1.mpi.isna()]



def _generate_mpi(unmatched: pd.DataFrame):
    """ Generate MPI for unmatched identities.  Update Master Person Long table """
    linklogger.info(f'Creating {len(unmatched)} identities')
    unmatched['mpi'] = unmatched.mpi.apply(generate_random_mpi)
    temp = unmatched.copy()
    temp.index = temp.mpi
    temp = temp.drop('mpi', axis=1)
    ident_inserts = gen_mpi_insert(temp)

    insert_objects = []
    for iarray in ident_inserts:
        insert_objects.extend([MasterPersonLong(**kwargs) for kwargs in iarray])

    with get_session() as session:
        session.bulk_save_objects(insert_objects)
        session.flush()
        session.commit()
    return unmatched



def _lookup_existing(t1: pd.DataFrame, t2: pd.DataFrame):
    linklogger.debug('Lookup Existing not implemented.  Returning all.')
    # raise NotImplementedError('Lookup Existing not implemented.')
    return None


def link(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    """  Link entrypoint.  Controls processes and intermediate data flow """
    matched, unmatched = _match(t1, t2)
    filled = _generate_mpi(unmatched=unmatched)
    return t1, t2



############################
### Record Linkage Steps ###
############################

## Preprocessing
def clean_raw(df: pd.DataFrame):
    obj_columns = df.select_dtypes(include='object').columns
    temp = df.copy()
    for col in obj_columns:
        if 'date' not in col:
            temp[col] = clean(temp[col])
    return temp


def match_dtype(dfa: pd.DataFrame, dfb: pd.DataFrame):
    df1 = dfa.copy()
    df2 = dfb.copy()
    for row in df1.dtypes.iteritems():
        col = row[0]
        if col in df2.columns:
            df2[col] = df2[col].astype(row[1])
    
    return df1, df2



## Indexing
### Helper Functions
def view_indices(ind, dfa, dfb):
    return dfa.loc[ind[0]], dfb.loc[ind[1]]

def calc_options(ind):
    return sum([i[1]==100 for i in ind])
###############

def _compare_in(colname, searchlist):
    cond1 = colname in searchlist
    cond2 = '_'.join(colname.split('_')[1:]) in searchlist
    return cond1 or cond2

def build_indexer(dview: pd.DataFrame):
    # Identify which columns to index and how to do so
    blocking_columns = [col for col in dview.columns if _compare_in(col, blocked_identifiers)]
    sngb_columns = [col for col in dview.columns if _compare_in(col, sneighbourhood_identifiers)]
    
    # Build the indexer
    indexer = Index()
    
    # Add sorted neighbour conditions
    for col in sngb_columns:
        indexer.add(SortedNeighbourhood(col))
    # Add blocking conditions
    for col in blocking_columns:
        indexer.add(Block(col, col))
    linklogger.info(f'Constructed indexer: \n{indexer.algorithms}')
    return indexer



## Comparing
def build_comparator(dview: pd.DataFrame):
    comparator = Compare(n_jobs=4)
    
    # Identify which columns to compare and how to do so
    blocking_columns = [col for col in dview.columns if _compare_in(col, blocked_identifiers)]
    sngb_columns = [col for col in dview.columns if _compare_in(col, sneighbourhood_identifiers)]
    
    # Use blocked columns as exact match criteria
    for col in blocking_columns:
        if is_numeric_dtype(dview[col]):
            comparator.numeric(col, col, label=col)
        else:
            comparator.exact(col, col, label=col)
    # Use neighbour columns as string jarowinkler comparisons
    for col in sngb_columns:
        comparator.string(col, col, method='jarowinkler', threshold=0.75, label=col)
        
    linklogger.info(f'Built comparator:\n {comparator.features}')
    return comparator



## Classifying


## Evaluating

