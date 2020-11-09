## Indexing

import pandas as pd
from assets.mapping import blocked_identifiers, sneighbourhood_identifiers
from recordlinkage import Index
from recordlinkage.index import Block, SortedNeighbourhood
from utils import compare_in

import logging

indexlogger = logging.getLogger(__name__)

def view_indices(ind, dfa, dfb):
    return dfa.loc[ind[0]], dfb.loc[ind[1]]

def calc_options(ind):
    return sum([i[1]==100 for i in ind])


def build_indexer(dview: pd.DataFrame, exclude=['gender_pool']):
    # Identify which columns to index and how to do so
    blocking_columns = [col for col in dview.columns if compare_in(col, blocked_identifiers)]
    sngb_columns = [col for col in dview.columns if compare_in(col, sneighbourhood_identifiers)]
    

    # Build the indexer
    indexer = Index()
    
    # Add sorted neighbour conditions
    for col in sngb_columns:
        if not compare_in(col, exclude):
            indexer.add(SortedNeighbourhood(col))
    # Add blocking conditions
    for col in blocking_columns:
        indexer.add(Block(col, col))
    indexlogger.info(f'Constructed indexer: \n{indexer.algorithms}')
    return indexer