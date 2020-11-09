#main.py

from utils import match_columns
from utils.db import init_db
from config import CLASSIFIER

from ingest import load_file
from mpi.prepare import create_distinct_view, create_identity_view
from mpi.preprocess import clean_raw, match_dtype
from mpi.link import is_match_available
from mpi.index import build_indexer
from mpi.compare import build_comparator
from mpi.classify import build_classifier, estimate_true
from mpi.update import generate_mpi, expand_match_to_raw
from mpi.evaluate import simple_evaluation

import logging 
logger = logging.getLogger(__name__)


def _load_from_db(tablename:str) -> dict:
    # Create a view of the data with mapped columns
    raw, subset = create_distinct_view(tablename=tablename)
    dview = subset.drop_duplicates()

    # Create a view from the MPI table with valid identity data
    iview = create_identity_view(mapped_columns=dview.columns.tolist())
    return {
        'raw': raw,
        'subset': subset,
        'dview':dview,
        'iview':iview,
    }


def _check_match(datapack:dict):
    dview = datapack['dview']
    iview = datapack['iview']
    # Check for match availability.  If not, hold process to generate MPIs from table.
    if not is_match_available(dview, iview):
        generate_mpi(dview)
        logger.info('Match unavailable.  Generated MPIs for data view.')
        # Recreate a view from the MPI table with valid identity data (reload from DB required after MPI creation)
        datapack['iview'] = create_identity_view(dview.columns.tolist())
    else:
        logger.info('Match available.  Proceed with linking process.')



def _preprocess(tablename:str):
    def _get_values(iview):
        if hasattr(iview, 'value'):
            return iview.value
        else:
            return iview
    def _get_scores(iview):
        if hasattr(iview, 'score'):
            return iview.score

    datapack = _load_from_db(tablename=tablename)
    _check_match(datapack=datapack)
    iview = datapack['iview']
    datapack['ivalue'] = _get_values(iview)
    datapack['iscore'] = _get_scores(iview)
    
    source_data, id_data = match_dtype(datapack['dview'], datapack['ivalue'])
    datapack['subset'] = clean_raw(datapack['subset'])
    datapack['source_clean'] = clean_raw(source_data)
    datapack['id_clean'] = clean_raw(id_data).reset_index(level='mpi')

    return datapack



def _index(datapack:dict):
    source_matched, id_matched = match_columns(
        t1=datapack['source_clean'],
        t2=datapack['id_clean'],
    )

    indexer = build_indexer(source_matched)
    datapack['candidates'] = indexer.index(source_matched, id_matched)
    datapack['source_matched'] = source_matched


def _compare(datapack:dict):
    comparator = build_comparator(datapack['source_matched'])
    datapack['comparisons'] = comparator.compute(
        pairs=datapack['candidates'],
        x=datapack['source_clean'],
        x_link=datapack['id_clean'],
    )



def _classify(datapack:dict):
    links_true = estimate_true(datapack['comparisons'])
    clf = build_classifier(
        name=CLASSIFIER,
        comparisons=datapack['comparisons'],
        match_index=links_true
    )
    datapack['links_true'] = links_true
    datapack['probabilities'] = clf.prob(comparison_vectors=datapack['comparisons'])
    datapack['predictions'] = clf.predict(comparison_vectors=datapack['comparisons'])



def _evaluate(datapack:dict):
    datapack['evaluations'] = simple_evaluation(
        source=datapack['source_clean'],
        links_true=datapack['links_true'],
        links_pred=datapack['predictions'],
        links_candidates=datapack['candidates'],
    )
    logger.info(f"Evaluations:\n{datapack['evaluations']}")



def _update(datapack:dict):
    output, matched, unmatched = expand_match_to_raw(
        raw=datapack['raw'],
        subset=datapack['subset'],
        source_clean=datapack['source_clean'],
        id_clean=datapack['id_clean'],
        links_pred=datapack['predictions'],
    )
    datapack['output'] = output
    datapack['matched'] = matched
    datapack['unmatched'] = unmatched



def _deidentify():
    pass


def run_mpi(tablename:str):
    datapack = _preprocess(tablename=tablename)
    _index(datapack=datapack)
    _compare(datapack=datapack)
    _classify(datapack=datapack)
    _evaluate(datapack=datapack)
    _update(datapack=datapack)

    return datapack




if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Ingest & MPI systems demo')

    parser.add_argument('--ingest')
    parser.add_argument('--tablename')
    parser.add_argument('--initdb', action='store_true')

    args = parser.parse_args()

    print(args)

    # Initialize Database if necessary
    if args.initdb:
        init_db()


    if args.ingest is not None:
        if args.tablename is not None:
            load_file(filename=args.ingest, tablename=args.tablename)
        else:
            raise ValueError('Ingest requires a --tablename attribute.  Use --ingest <filename> --tablename <tablename>')

    

    # dview = create_distinct_view('ushe_students')
    # print(dview.head())

    # iview = create_identity_view(dview.columns.tolist())
    # print(iview)

    # print(link(dview, iview))