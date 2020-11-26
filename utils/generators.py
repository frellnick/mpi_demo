# generators.py


import random
import time
import pandas as pd
from itertools import combinations
import math

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

def _prepare_sql_inserts(index, valdict):
    """
    Build a mpi-field-value-guid insert for each key in an raw vector
    """
    inserts = []
    for key in valdict:
        inserts.append(
            {
                'mpi': valdict['mpi'],
                'guid': valdict['guid'],
                'field': key,
                'value': valdict[key]
            }
        )
    return inserts
insertGenerators['sql'] = _prepare_sql_inserts



def _prepare_nosql_inserts(index, valdict):
    """
    Return full data in form for serializtion into NoSQL schema.
    """
    if index in valdict:
        valdict.pop('index')
    return valdict
insertGenerators['nosql'] = _prepare_nosql_inserts



def gen_mpi_insert(iinfo: pd.DataFrame):
    temp = iinfo.copy()
    gutillogger.debug(f"Generating inserts for dataframe of len {len(temp)}.  Columns {temp.columns}")
    gutillogger.debug(f"Example MPI: {temp.mpi[0:5]}")

    prepare_inserts = insertGenerators[MPIARCH]

    idict = temp.to_dict('index')
    for index in idict:
        yield prepare_inserts(index, idict[index])


##############################
### NoSQL Vector Generator ###
##############################


## Frequency Functions

def mean(proportions):
    return sum(proportions) / len(proportions)

def geomean(proportions):
    return math.prod(proportions) ** (1/len(proportions))


## Vectorizer

def create_mpi_vector(mdoc:dict, freqfn=geomean) -> list:
    
    def _extract_fields(mdoc:dict) -> list:
        fields = []
        for s in mdoc['sources']:
            fields.extend(s['fields'])
        return fields
    
    
    def _convert_dict_to_tuple(d:dict)->tuple:
        return tuple(d.values())
    
    
    def _index_and_count_values(values:tuple)->dict:
        index = {}
        counts = {}
        for i, v in enumerate(values):
            if v[0] in index:
                index[v[0]].append(i)
            else:
                index[v[0]] = [i]
            if v[1] in counts:
                counts[v[1]] += 1
            else:
                counts[v[1]] = 1
        return index, counts
        
    
    def _build_vectors(values: dict, index: dict, counts:dict, freqfn) -> list:
        def _is_valid_vect(c:tuple) -> bool:
            vnames = [item[0] for item in c]
            return len(set(vnames)) == len(vnames)
        
        def _consolidate_tuples_to_dict(c:tuple) -> dict:
            res = {}
            [res.update({f[0]:f[1]}) for f in c]
            return res
        
        def _calc_freq_score(vect:dict, counts:dict, index:dict, freqfn) -> float:
            proportions = []
            for key in vect:
                proportions.append(
                    counts[vect[key]] / len(index[key])
                )
            return freqfn(proportions)
        
        fnames = tuple(index.keys())
        cmb = combinations(set(values), len(fnames))
        vectors = [_consolidate_tuples_to_dict(c) for c in cmb if _is_valid_vect(c)]
        [v.update({'freq_score': _calc_freq_score(v, counts, index, freqfn)}) for v in vectors]
        return vectors

    
    def _add_mpi_to_vects(vectors:list, mpi) -> list:
        for v in vectors:
            v.update({'mpi': mpi})
        return vectors
    
    
    del(mdoc['_id'])
    values = [_convert_dict_to_tuple(d) for d in _extract_fields(mdoc)]
    index, counts = _index_and_count_values(values)
    mvects = _build_vectors(values=values, index=index, counts=counts, freqfn=freqfn)
    mvects = _add_mpi_to_vects(mvects, mdoc['mpi'])
    return mvects