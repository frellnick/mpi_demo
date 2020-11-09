# update.py

from utils.mpiutils import union_frames
import pandas as pd
import numpy as np
from utils.db import get_session
from utils.generators import generate_random_mpi, gen_mpi_insert
from utils.models import MasterPersonLong


import logging

updatelogger = logging.getLogger(__name__)

def _lookup_mpi(ind: int, iview: pd.DataFrame, match_dict):
    if ind in match_dict:
        return iview.iloc[match_dict[ind]].mpi
    else:
        return np.nan

def _multiindex_to_dict(iterable):
    temp = {}
    for pair in iterable:
        temp[pair[0]] = pair[1]
    return temp


def append_mpi(dview, iview, matches):
    dview['mpi'] = dview.index.to_numpy()
    match_dict = _multiindex_to_dict(matches)
    dview['mpi'] = dview['mpi'].apply(_lookup_mpi, args=(iview, match_dict))
    
    # Split dview into matched and unmatched
    matched = dview[dview.mpi.notna()]
    unmatched = dview[dview.mpi.isna()]
    return matched, unmatched



def generate_mpi(unmatched: pd.DataFrame):
    """ Generate MPI for unmatched identities.  Update Master Person Long table """
    updatelogger.info(f'Creating {len(unmatched)} identities')
    temp = unmatched.copy()
    if 'mpi' not in temp.columns:
        temp['mpi'] = None
    temp['mpi'] = temp.mpi.apply(generate_random_mpi)
    ident_inserts = gen_mpi_insert(temp)

    insert_objects = []
    for iarray in ident_inserts:
        insert_objects.extend([MasterPersonLong(**kwargs) for kwargs in iarray])

    with get_session() as session:
        session.bulk_save_objects(insert_objects)
        session.flush()
        session.commit()
    return temp



def expand_match_to_raw(raw, subset, dview, iview, links_pred):
    """
        Params
        raw:    unaltered select statement from source table
        subset: identiy view of raw source table (columns renamed)
        dview: deduped subset, processed during linkage
        iview: deduped identity view, processed during linkage
        links_pred: multi-index from classification step
    """
    matched, unmatched = append_mpi(dview, iview, links_pred)
    updatelogger.info(f"Matched {len(matched)} records.  Unmatched {len(unmatched)} records.")
    unmatched = generate_mpi(unmatched)
    updatelogger.debug(f"Unmatched frame showing {len(unmatched[unmatched.mpi.notna()])} \
        of {len(unmatched)} MPI generated.")
    dview = union_frames(matched, unmatched)
    updatelogger.info(f"Union dview frame shows {len(dview[dview.mpi.notna()])} mpi.")

    std_columns = [col for col in dview if col != 'mpi']

    # Expand deduped matches to full identity table
    mapped_ids = pd.merge(subset, dview, how='left', left_on=std_columns, right_on=std_columns)

    # Match mpi with corresponding indices in raw table
    raw['mpi'] = mapped_ids.mpi
    updatelogger.info(f'Final output {len(raw[raw.mpi.notna()])} of {len(raw)} records assigned MPI')
    return raw, matched, unmatched