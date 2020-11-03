"""
link.py

linking workflow.  checks prepared records and runs through link sequence similar to:

blocking, indexing, comparing, classifying
"""

import pandas as pd
import numpy as np 

from utils import union_frames
from utils.generators import generate_random_mpi, gen_mpi_insert
from utils.db import get_session
from utils.models import MasterPersonLong

from mpi.preprocess import clean_raw, match_dtype
from mpi.index import build_indexer
from mpi.compare import build_comparator
from mpi.classify import build_classifier, estimate_true
from mpi.update import append_mpi, expand_match_to_raw

import logging

linklogger = logging.getLogger(__name__)



#######################
## Driver Functions ###
#######################

def link(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    """Link entrypoint.  Controls processes and intermediate data flow """
    matched, unmatched = match(t1, t2)
    unmatched = generate_mpi(unmatched=unmatched)
    return union_frames(matched, unmatched)


def match(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    """ Block, Index, Compare, Classify records across both frames """
    return t1[t1.mpi.notna()], t1[t1.mpi.isna()]


def generate_mpi(unmatched: pd.DataFrame):
    """ Generate MPI for unmatched identities.  Update Master Person Long table """
    linklogger.info(f'Creating {len(unmatched)} identities')
    if 'mpi' not in unmatched.columns:
        unmatched['mpi'] = None
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



######################
### Process Checks ###
######################
def _check_col(base, column_names):
    return sum([base in col for col in column_names]) > 0

def _check_numeric(available_columns):
    dets = ['ssn', 'ssid']
    a = list(available_columns)
    return sum([_check_col(d, a) for d in dets]) > 0

def _check_demographic(available_columns):
    req_set = ['first', 'last', 'birth']
    a = list(available_columns)
    return sum([_check_col(r, a) for r in req_set]) == len(req_set)

def is_match_available(dview, iview):
    dcol = set(list(dview.columns))
    if hasattr(iview, 'value'):
        ivcols = iview.value.columns
    else:
        ivcols = iview.columns
    icol = set(list(ivcols))

    available_columns = dcol.intersection(icol)
    
    return _check_numeric(available_columns) or _check_demographic(available_columns)