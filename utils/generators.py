# generators.py


import random
import time
import pandas as pd

import logging

gutillogger = logging.getLogger(__name__)

def generate_random_mpi(*args):
    components = []
    for _ in range(4):
        components.append(
            str(random.getrandbits(24))
        )
    return '-'.join(components)


def gen_mpi_insert(iinfo: pd.DataFrame):
    temp = iinfo.copy()
    gutillogger.debug(f"Generating inserts for dataframe of len {len(temp)}.  Columns {temp.columns}")
    gutillogger.debug(f"Example MPI: {temp.mpi[0:5]}")
    if 'mpi' in temp.columns:
        temp.index = temp.mpi
        temp = temp.drop('mpi', axis=1)
    def _expand_row(mpi, valdict):
        inserts = []
        for key in valdict:
            inserts.append(
                {
                    'mpi': mpi,
                    'guid': valdict['guid'],
                    'score': 1,
                    'field': key,
                    'value': valdict[key]
                }
            )
        return inserts

    idict = temp.to_dict('index')
    for mpi in idict:
        yield _expand_row(mpi, idict[mpi])