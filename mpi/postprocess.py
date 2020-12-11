# postprocess.py


import pandas as pd

from db import dataframe_to_db, get_session
from db.common import get_table_columns
from assets.mapping import blocked_identifiers

import logging

### Flags And Standard Reports ### 

class Flag():
    def __init__(self, checkfn, report, write):
        self.check = checkfn
        self.report = report 
        self.write = write 

    def run(self, *args, **kwargs):
        res = self.check(*args, **kwargs)
        self.report(res)
        self.write(res)



class Report():
    def __init__(self, report_fn, name=None):
        self.template = {'flag', 'mpi', 'notes'}
        self.report_fn = report_fn
        self.logger = logging.getLogger(f'{__name__}_{name}')
    
    
    def _validate(self, resultframe: pd.DataFrame) -> bool:
        for key in self.template:
            if key not in resultframe.columns:
                return False
        return True

    
    def _log_report(self, resultframe: pd.DataFrame):
        rep, err = self.report_fn(resultframe)
        if err is None:
            self.logger.debug(f'{rep}')
        else:
            self.logger.error(err)
        return err
            

    def _write_results(self, resultframe: pd.DataFrame):
        err = dataframe_to_db(resultframe, tablename='flags', if_exists='append')
        return err


    def __call__(self, resultframe: pd.DataFrame):
        if self._validate(resultframe):
            err = self._log_report(resultframe)
            err = self._write_results(resultframe)
            if err is not None:
                self.logger.error(f'{err}')
        
            
            



## Check Functions (Rule1 vs Rule2) ##

def check_repeat_identifiers(*args, **kwargs) -> pd.DataFrame:
    
    def _build_cte_query_from_column(colname):
        q = \
            f"flag_{colname} AS (SELECT mpi FROM mpi_vectors GROUP BY {colname} HAVING COUNT(mpi) > 1)"
        return q

    def _build_ctes(available_columns):
        return ',\n'.join([_build_cte_query_from_column(col) for col in available_columns])

    def _build_unions(available_columns):
        if len(available_columns) > 1:
            return '\n'.join([f'UNION SELECT mpi FROM flag_{col}' for col in available_columns[1:]])
        else:
            return ''

    def _compile_query(ctes, unions, available_columns):
        try:
            return f"WITH \n{ctes} \nSELECT mpi FROM flag_{available_columns[0]} {unions};"
        except Exception as e:
            raise e

    table_columns, err = get_table_columns('mpi_vectors')
    available_columns = [col+'_pool' for col in table_columns if col in blocked_identifiers]
    if err is None:
        if len(available_columns) == 0: 
            return []
        ctes = _build_ctes(available_columns)
        unions = _build_unions(available_columns)
        return  _compile_query(ctes, unions, available_columns)
    else:
        raise ValueError(err)
    

    



## Report Functions (Log Stats)

def simple_count(resultframe: pd.DataFrame) -> dict:
    return {
        'CountFlagged': len(resultframe)
    }

