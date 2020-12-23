"""
link.py

linking workflow.  checks prepared records and runs through link sequence similar to:

blocking, indexing, comparing, classifying
"""

import logging

linklogger = logging.getLogger(__name__)

from utils import get_column_intersect


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
    print('checking available columns', available_columns)
    return (_check_numeric(available_columns) or _check_demographic(available_columns)) \
                and (len(iview) > 1)