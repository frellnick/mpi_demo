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
    available_columns = get_table_columns('mpi_vector')
    query = """
        SELECT 
            mpi
        FROM 
            mpi_vectors
        WHERE

    """


## Report Functions (Log Stats)

def simple_report(resultframe: pd.DataFrame) -> dict:
    return {
        'CountFlagged': len(resultframe)
    }

