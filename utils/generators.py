# generators.py


import random
import time
import pandas as pd

from config import MPIARCH

import logging

gutillogger = logging.getLogger(__name__)

def generate_random_mpi(*args):
    components = []
    for _ in range(4):
        components.append(
            str(random.getrandbits(24))
        )
    return '-'.join(components)


#########################
### Insert Generators ###
#########################

insertGenerators = {}

def _prepare_sql_inserts(mpi, valdict):
    """
    Build a mpi-field-value-guid insert for each key in an raw vector
    """
    inserts = []
    for key in valdict:
        inserts.append(
            {
                'mpi': mpi,
                'guid': valdict['guid'],
                'field': key,
                'value': valdict[key]
            }
        )
    return inserts
insertGenerators['sql'] = _prepare_sql_inserts



def _prepare_nosql_inserts(mpi, valdict):
    """
    Return full data in form for serializtion into NoSQL schema.
    """
    return {
        'mpi': mpi,
        'data': valdict, 
    }
insertGenerators['nosql'] = _prepare_nosql_inserts



def gen_mpi_insert(iinfo: pd.DataFrame):
    temp = iinfo.copy()
    gutillogger.debug(f"Generating inserts for dataframe of len {len(temp)}.  Columns {temp.columns}")
    gutillogger.debug(f"Example MPI: {temp.mpi[0:5]}")
    if 'mpi' in temp.columns:
        temp.index = temp.mpi
        temp = temp.drop('mpi', axis=1)

    prepare_inserts = insertGenerators[MPIARCH]

    idict = temp.to_dict('index')
    for mpi in idict:
        yield prepare_inserts(mpi, idict[mpi])