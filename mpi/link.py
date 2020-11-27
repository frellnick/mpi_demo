"""
link.py

linking workflow.  checks prepared records and runs through link sequence similar to:

blocking, indexing, comparing, classifying
"""

import pandas as pd
import numpy as np 

from utils import union_frames, get_column_intersect
from mpi.index import build_indexer
from mpi.compare import build_comparator
from mpi.classify import build_classifier, estimate_true
from mpi.update import append_mpi, expand_match_to_raw

import logging

linklogger = logging.getLogger(__name__)



#######################
## Driver Functions ###
#######################



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
    available_columns = get_column_intersect(dview, iview)
    return (_check_numeric(available_columns) or _check_demographic(available_columns)) \
                and (len(iview) > 1)