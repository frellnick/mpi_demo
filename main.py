#main.py

from assets.mapping import colmap

from utils import match_dataframe_columns
from db import init_db, dataframe_to_db
from utils.exceptions import Bypass

from config import CLASSIFIER

from ingest import load_file
from mpi.prepare import create_data_view, create_identity_view
from mpi.preprocess import clean_raw, match_dtype
from mpi.link import is_match_available
from mpi.index import build_indexer
from mpi.compare import build_comparator
from mpi.classify import build_classifier, estimate_true
from mpi.update import (
    generate_mpi, expand_match_to_raw, update_mpi_vector_table, write_mpi_data, gen_mpi_insert
)
from mpi.evaluate import simple_evaluation

from di import simple_di


import logging 
logger = logging.getLogger(__name__)


def _load_from_db(tablename:str) -> dict:
    # Create a view of the data with mapped columns
    raw, subset = create_data_view(tablename)
    dview = subset.drop_duplicates()
    iview = create_identity_view(mapped_columns=dview.columns.to_list())
    logger.debug(f"Datapack created:\n\
            dview - len:{len(dview)}\n\
            iview - len:{len(iview)}")
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
    logging.debug(f'Checking for match potential.\
            DVIEW: len - {len(dview)}, columns - {dview.columns}\
            IVIEW: len - {len(iview)}, columns - {iview.columns}')
    if is_match_available(dview, iview):
        logger.info('Match available.  Proceed with linking process.')
    else:
        logger.info('Match unavailable.  Generated MPIs for data view.')
        temp = generate_mpi(
            clean_raw(dview)
        )
        write_mpi_data(gen_mpi_insert(temp))
        update_mpi_vector_table()
        # Recreate a view from the MPI table with valid identity data
        iview = create_identity_view(mapped_columns=dview.columns.tolist())
        return Bypass()
    


def _preprocess(tablename:str):
    datapack = _load_from_db(tablename=tablename)
    err = _check_match(datapack=datapack)
    source_data, id_data = match_dtype(datapack['dview'], datapack['iview'])
    datapack['subset'] = clean_raw(datapack['subset'])
    datapack['source_clean'] = clean_raw(source_data)
    datapack['id_clean'] = clean_raw(id_data)
    return datapack, err



def _index(datapack:dict):
    source_matched, id_matched = match_dataframe_columns(
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

    update_mpi_vector_table()


def _deidentify(datapack:dict, tablename:str):
    dataframe_to_db(
        simple_di(datapack['output']), 
        tablename=tablename + '_di'
    )


def run_mpi(tablename:str):
    logger.info(f"Running MPI on table: {tablename}")
    datapack, err = _preprocess(tablename=tablename)
    if err is not None:
            logger.warn(f'Error detected. \n{err}')
            return datapack
    _index(datapack=datapack)
    _compare(datapack=datapack)
    _classify(datapack=datapack)
    _evaluate(datapack=datapack)
    _update(datapack=datapack)
    _deidentify(datapack=datapack, tablename=tablename)

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