# postprocess.py


import pandas as pd

from db import dataframe_to_db, get_session, query_db
from db.common import get_table_columns
from assets.mapping import blocked_identifiers
from utils.filters import search_list
from utils import result_proxy_to_dataframe

import logging

postlog = logging.getLogger(__name__)

### Flags And Standard Reports ### 

class Flag():
    def __init__(self, checkfn, report, write, name):
        self.check = checkfn
        self.report = report
        self.write = write
        self.name = name

    def run(self, *args, **kwargs):
        res = self.check(*args, **kwargs)
        self._res = res
        self.report(res)
        if self.write is not None:
            self.write(res)

    @property
    def result(self):
        if hasattr(self, '_res'):
            return self._res
        return None

    def __call__(self, *args, **kwargs):
        postlog.info(f"Starting flag {self.name}")
        self.run(*args, **kwargs)
        return self

            

## Check Functions (Rule1 vs Rule2) ##

#TODO: Is it worth it to begin generalizing these types of SQL constructors?  May not be. Something to look for if more cases arise.

# Rule 1 check function - Looks for MPIs with dissimilar blocked identifiers in its record
def check_dissimilar_identifiers(*args, **kwargs) -> pd.DataFrame:

    def _build_cte_query_from_column(colname):
        return \
            f"flag_{colname} AS (SELECT mpi\n\
                FROM (SELECT DISTINCT mpi, {colname} FROM mpi_vectors WHERE {colname} IS NOT NULL) \n\
                    GROUP BY mpi HAVING COUNT(mpi) > 1)"

    def _build_ctes(available_columns):
        return ',\n'.join([_build_cte_query_from_column(col) for col in available_columns])

    def _build_unions(available_columns):
        if len(available_columns) > 1:
            return '\n'.join([f"UNION SELECT mpi FROM flag_{col}" for col in available_columns[1:]])
        else:
            return ''

    def _compile_query(ctes, unions, available_columns):
        try:
            return f"WITH \n{ctes} \nSELECT mpi FROM flag_{available_columns[0]} {unions};"
        except Exception as e:
            raise e

    table_columns, err = get_table_columns('mpi_vectors')
    available_columns = [col for col in table_columns if search_list(col, blocked_identifiers)]

    table_columns, err = get_table_columns('mpi_vectors')
    available_columns = [col for col in table_columns if search_list(col, blocked_identifiers)]
    if err is None:
        if len(available_columns) == 0: 
            postlog.warning('Could not find any available columns to run check in MPI Vectors')
            return pd.DataFrame()
        ctes = _build_ctes(available_columns)
        unions = _build_unions(available_columns)
        query = _compile_query(ctes, unions, available_columns)
        return result_proxy_to_dataframe(query_db(query).fetchall())
    else:
        raise ValueError(err)



# Rule 2 check function - Looks for mpis that share an identifier
def check_repeat_identifiers(*args, **kwargs) -> pd.DataFrame:
    
    def _build_cte_query_from_column(colname):
        return \
            f"flag_{colname} AS (SELECT GROUP_CONCAT(mpi, ',') AS mpi, '{colname}' AS field, {colname} AS value \n\
                FROM (SELECT DISTINCT mpi, {colname} FROM mpi_vectors) \n\
                    WHERE {colname} IS NOT NULL \n\
                        GROUP BY {colname} HAVING COUNT(mpi) > 1)"

    def _build_ctes(available_columns):
        return ',\n'.join([_build_cte_query_from_column(col) for col in available_columns])

    def _build_unions(available_columns):
        if len(available_columns) > 1:
            return '\n'.join([f"UNION SELECT mpi, field, value FROM flag_{col}" for col in available_columns[1:]])
        else:
            return ''

    def _compile_query(ctes, unions, available_columns):
        try:
            return f"WITH \n{ctes} \nSELECT mpi, field, value FROM flag_{available_columns[0]} {unions};"
        except Exception as e:
            raise e

    table_columns, err = get_table_columns('mpi_vectors')
    available_columns = [col for col in table_columns if search_list(col, blocked_identifiers)]
    if err is None:
        if len(available_columns) == 0: 
            postlog.warning('Could not find any available columns to run check in MPI Vectors')
            return pd.DataFrame()
        ctes = _build_ctes(available_columns)
        unions = _build_unions(available_columns)
        query = _compile_query(ctes, unions, available_columns)
        return result_proxy_to_dataframe(query_db(query).fetchall())
    else:
        raise ValueError(err)


## Report Functions (Log Stats)

def simple_count(resultframe: pd.DataFrame) -> dict:
    return {
        'CountFlagged': len(resultframe)
    }




## Report Transformation Functions

def add_flag_name(name):
    def apply_fn(df: pd.DataFrame, name=name):
        postlog.debug(f"Adding column flag with value {name}")
        df['flag'] = name
        return df
    return apply_fn


## Assemble known flags
Rule1 = Flag(
    checkfn = check_dissimilar_identifiers,
    report = simple_count,
    write = None,
    name = 'Rule1',
)


Rule2 = Flag(
    checkfn = check_repeat_identifiers,
    report = simple_count,
    write=None,
    name='Rule2',
)
