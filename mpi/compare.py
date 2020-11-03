## Comparing

import pandas as pd
from recordlinkage import Compare
from pandas.api.types import is_numeric_dtype
from utils import compare_in
from assets.mapping import blocked_identifiers, sneighbourhood_identifiers

import logging

comparelogger = logging.getLogger(__name__)



def build_comparator(dview: pd.DataFrame):
    comparator = Compare(n_jobs=4)
    
    # Identify which columns to compare and how to do so
    blocking_columns = [col for col in dview.columns if compare_in(col, blocked_identifiers)]
    sngb_columns = [col for col in dview.columns if compare_in(col, sneighbourhood_identifiers)]
    
    # Use blocked columns as exact match criteria
    for col in blocking_columns:
        if is_numeric_dtype(dview[col]):
            comparator.numeric(col, col, label=col)
        else:
            comparator.exact(col, col, label=col)
    # Use neighbour columns as string jarowinkler comparisons
    for col in sngb_columns:
        comparator.string(col, col, method='jarowinkler', threshold=0.75, label=col)

    comparelogger.info(f'Built comparator:\n {comparator.features}')
    return comparator