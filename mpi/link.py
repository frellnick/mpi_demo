"""
link.py

linking workflow.  checks prepared records and runs through link sequence similar to:

blocking, indexing, comparing, classifying
"""

import pandas as pd
from utils.generators import generate_random_mpi

import logging

linklogger = logging.getLogger(__name__)



def _match(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    """ Block, Index, Compare, Classify records across both frames """
    d1 = t1.copy()
    if len(t2) == 0:
        d1['mpi'] = None
    else:
        _lookup_existing(t1, t2)
    return d1



def _generate_mpi(unmatched: pd.DataFrame):
    """ Generate MPI for unmatched identities.  Update Master Person Long table """
    linklogger.info(f'Creating {len(unmatched)} identities')
    temp = unmatched.copy()
    temp['mpi'] = temp.mpi.apply(generate_random_mpi)



def _lookup_existing(t1: pd.DataFrame, t2: pd.DataFrame):
    linklogger.debug('Lookup Existing not implemented')
    raise NotImplementedError('Lookup Existing not implemented.')    



def link(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    """  Link entrypoint.  Controls processes and intermediate data flow """
    matched_frame = _match(t1, t2)
    unmatched = matched_frame[matched_frame['mpi'].isna()]
    _generate_mpi(unmatched=unmatched)