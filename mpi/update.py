# update.py

import pandas as pd
import numpy as np

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
    return dview


def expand_match_to_raw(raw, subset, dview, iview, links_pred):
    """
        Params
        raw:    unaltered select statement from source table
        subset: identiy view of raw source table (columns renamed)
        dview: deduped subset, processed during linkage
        iview: deduped identity view, processed during linkage
        links_pred: multi-index from classification step
    """
    dview = append_mpi(dview, iview, links_pred)
    std_columns = [col for col in dview if col != 'mpi']

    # Expand deduped matches to full identity table
    mapped_ids = pd.merge(subset, dview, how='left', left_on=std_columns, right_on=std_columns)

    # Match mpi with corresponding indices in raw table
    raw['mpi'] = mapped_ids.mpi
    return raw

