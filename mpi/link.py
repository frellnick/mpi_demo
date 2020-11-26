"""
link.py

linking workflow.  checks prepared records and runs through link sequence similar to:

blocking, indexing, comparing, classifying
"""

import pandas as pd
import numpy as np 

from utils import union_frames, match_columns

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
    # unmatched = generate_mpi(unmatched=unmatched)
    raise NotImplementedError
    return union_frames(matched, unmatched)


def match(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    """ Block, Index, Compare, Classify records across both frames """
    return t1[t1.mpi.notna()], t1[t1.mpi.isna()]



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
    
    return (_check_numeric(available_columns) or _check_demographic(available_columns)) \
                and (len(iview) > 1)