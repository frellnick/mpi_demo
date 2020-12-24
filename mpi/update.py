# update.py

from utils import (
    union_frames,
    generate_random_mpi, gen_mpi_insert, create_mpi_vector,
    extract_dataframe
)

from db import (
    get_session, get_mongo, yield_mpi_document_batch, dataframe_to_db,
    write_mpi_data
)

from mpi.prepare import View

from assets.mapping import colmap

import pandas as pd
import numpy as np

import logging

updatelogger = logging.getLogger(__name__)


###############################
### Update MPI Vector Table ###
###############################


def _create_mpi_vectors(*args, **kwargs) -> list:
    doc_chunks = yield_mpi_document_batch()
    mpi_vectors = []
    for chunk in doc_chunks:
        for doc in chunk:
            new_vectors = create_mpi_vector(doc)
            mpi_vectors.extend(new_vectors)

    return mpi_vectors


def _detect_add_column_names(colmap: dict, raw_columns:list, optional:list, exclude:list):
    columns = set(colmap.values())
    for name in optional:
        columns.add(name)
    for name in raw_columns:
        columns.add(name)
    for name in exclude:
        columns.remove(name)
    return columns


def update_mpi_vector_table():
    """
    Replace MPI Vectors table with new MPI vectors.
        TODO: Alter find to only look for inserted or updated ids*
        TODO: Alter write to only truncate/add MPIs returned from previous step
        NOTE: mpi is a indexed, constrained unique.  1 _id === 1 mpi
    """
    updatelogger.info("Starting MPI Vector table update.")
    
    vectors = _create_mpi_vectors()
    updatelogger.info(f"Generated {len(vectors)} vectors.")

    if len(vectors) > 0:
        columns = _detect_add_column_names(
            colmap=colmap,
            raw_columns=list(vectors[0].keys()),
            optional=['mpi', 'freq_score'],
            exclude=['guid']
        )
    
        df = pd.DataFrame.from_records(data=vectors, columns = columns)
        dataframe_to_db(df, 'mpi_vectors')



def write_matched_unmatched(matched: pd.DataFrame, unmatched: pd.DataFrame):
    # Update Identity Pool by writing updates or new entries
    try:
        if len(matched) > 0:
            write_mpi_data(gen_mpi_insert(matched), update=True)
    except Exception as e:
        updatelogger.error(f'Could not write matched data to MPI table. {e}')
        raise e
    try:
        if len(unmatched) > 0:
            write_mpi_data(gen_mpi_insert(unmatched), update=False)
    except Exception as e:
        updatelogger.error(f'Could not write unmatched data to MPI table. {e}')
        raise e
    

##################################
### Lookup & Generation of MPI ###
##################################

def _lookup_mpi(ind: int, id_clean: pd.DataFrame, match_dict):
    if ind in match_dict:
        try:
            return id_clean.loc[match_dict[ind]].mpi
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



def append_mpi(source_clean: pd.DataFrame, id_clean: pd.DataFrame, links_pred: pd.MultiIndex) -> tuple:
    """
        Append MPI

        Params
        source_clean: deduped subset, processed during linkage
        id_clean: deduped identity view, processed during linkage
        links_pred: multi-index from classification step
    """
    def _match_available(source_clean, id_clean, matches) -> tuple:
        source_clean['mpi'] = source_clean.index.to_numpy()
        match_dict = _multiindex_to_dict(matches)
        source_clean['mpi'] = source_clean['mpi'].apply(_lookup_mpi, args=(id_clean, match_dict))
        
        # Split source_clean into matched and unmatched
        matched = source_clean[source_clean.mpi.notna()]
        unmatched = source_clean[source_clean.mpi.isna()]
        return matched, unmatched

    matched, unmatched = _match_available(source_clean, id_clean, links_pred)
    updatelogger.info(f"Matched {len(matched)} records.  Unmatched {len(unmatched)} records.")

    # Generate MPI's for unmatched rows
    unmatched = generate_mpi(unmatched)
    updatelogger.debug(f"Unmatched frame showing {len(unmatched[unmatched.mpi.notna()])} \
        of {len(unmatched)} MPI generated.")
    
    return matched, unmatched



def generate_mpi(unmatched: pd.DataFrame):
    """ Generate MPI for unmatched identities. """
    updatelogger.info(f'Creating {len(unmatched)} identities')
    temp = extract_dataframe(unmatched).copy()
    if 'mpi' not in temp.columns:
        temp['mpi'] = None
    temp['mpi'] = temp.mpi.apply(generate_random_mpi)
    return temp



def _remove_index_col(dataframe: pd.DataFrame) -> pd.DataFrame:
    if 'index' in dataframe.columns:
        return dataframe.drop(['index'], axis=1)
    return dataframe



def expand_match_to_view(view: View, combined: pd.DataFrame):
    """
        Expand Match to View
        Params
        view: Data view of source dataset
        combined: Deduplicated matched dataframe
    """

    # Expand deduped matches to full identity table
    mapped = view.merge(combined)
    updatelogger.info(f'Final output {len(mapped[mapped.mpi.notna()])} of {len(view)} records assigned MPI')
    return mapped