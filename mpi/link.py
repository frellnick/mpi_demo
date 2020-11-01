"""
link.py

linking workflow.  checks prepared records and runs through link sequence similar to:

blocking, indexing, comparing, classifying
"""

import pandas as pd
from utils.generators import generate_random_mpi, gen_mpi_insert
from utils.db import get_session
from utils.models import MasterPersonLong

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