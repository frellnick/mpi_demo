# generators.py


import random
import time
import pandas as pd

def generate_random_mpi(*args):
    components = []
    for _ in range(4):
        components.append(
            str(random.getrandbits(24))
        )
    return '-'.join(components)


def gen_mpi_insert(iinfo: pd.DataFrame):
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

    idict = iinfo.to_dict('index')
    for mpi in idict:
        yield _expand_row(mpi, idict[mpi])