# update.py

from utils.mpiutils import union_frames
import pandas as pd
import numpy as np
from utils.db import get_session
from utils.generators import generate_random_mpi, gen_mpi_insert
from utils.models import MasterPersonLong


import logging

updatelogger = logging.getLogger(__name__)

def _lookup_mpi(ind: int, id_clean: pd.DataFrame, match_dict):
    if ind in match_dict:
        try:
            return id_clean.iloc[match_dict[ind]].mpi
        except TypeError as e:
            updatelogger.error(f'Could not index MPI.\nIDX: {ind}\ id_clean:\n {id_clean.head()}\nmatch_dict[ind]: {match_dict[ind]}')
            raise e
    else:
        return np.nan

def _multiindex_to_dict(iterable):
    temp = {}
    for pair in iterable:
        temp[pair[0]] = pair[1]
    return temp


def append_mpi(source_clean, id_clean, matches):
    source_clean['mpi'] = source_clean.index.to_numpy()
    match_dict = _multiindex_to_dict(matches)
    source_clean['mpi'] = source_clean['mpi'].apply(_lookup_mpi, args=(id_clean, match_dict))
    
    # Split source_clean into matched and unmatched
    matched = source_clean[source_clean.mpi.notna()]
    unmatched = source_clean[source_clean.mpi.isna()]
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


def _remove_index_col(dataframe: pd.DataFrame) -> pd.DataFrame:
    if 'index' in dataframe.columns:
        return dataframe.drop(['index'], axis=1)
    return dataframe


def expand_match_to_raw(raw, subset, source_clean, id_clean, links_pred):
    """
        Params
        raw:    unaltered select statement from source table
        subset: identiy view of raw source table (columns renamed)
        source_clean: deduped subset, processed during linkage
        id_clean: deduped identity view, processed during linkage
        links_pred: multi-index from classification step
    """
    matched, unmatched = append_mpi(source_clean, id_clean, links_pred)
    updatelogger.info(f"Matched {len(matched)} records.  Unmatched {len(unmatched)} records.")
    unmatched = generate_mpi(unmatched)
    updatelogger.debug(f"Unmatched frame showing {len(unmatched[unmatched.mpi.notna()])} \
        of {len(unmatched)} MPI generated.")
    source_clean = union_frames(matched, unmatched)
    updatelogger.info(f"Union source_clean frame shows {len(source_clean[source_clean.mpi.notna()])} mpi.")

    std_columns = [col for col in source_clean if col != 'mpi']

    # Expand deduped matches to full identity table
    mapped_ids = pd.merge(subset, source_clean, how='left', left_on=std_columns, right_on=std_columns)

    # Match mpi with corresponding indices in raw table
    raw = _remove_index_col(raw)
    raw['mpi'] = mapped_ids.mpi
    updatelogger.info(f'Final output {len(raw[raw.mpi.notna()])} of {len(raw)} records assigned MPI')
    return raw, matched, unmatched