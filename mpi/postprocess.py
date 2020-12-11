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
    def __init__(self, checkfn, report, write):
        self.check = checkfn
        self.report = report
        self.write = write 

    def run(self, *args, **kwargs):
        res = self.check(*args, **kwargs)
        self.report(res)
        if self.write is not None:
            self.write(res)

    def __call__(self, *args, **kwargs):
        self.run(*args, **kwargs)



class Report():
    def __init__(self, report_fn, transform=None):
        self.template = {'flag', 'mpi', 'notes'}
        self.report_fn = report_fn
        self.transform = transform
        self.logger = logging.getLogger(f'{__name__}')
    
    
    def _validate(self, resultframe: pd.DataFrame) -> bool:
        for key in self.template:
            if key not in resultframe.columns:
                return False
        return True

    def _transform(self, resultframe: pd.DataFrame):
        err = None
        if self.transform is None:
            self._transformed_results = resultframe
        else:
            try:
                self._transformed_results = self.transform(resultframe)
            except Exception as e:
                err = e
                self._transformed_results = resultframe
                self.logger.error(f'{e}')
        return self._transformed_results, err

    
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
            tdf, err = self._transform((resultframe))
            if err is not None:
                err = self._write_results(tdf)
            return tdf, err
        
            
            



## Check Functions (Rule1 vs Rule2) ##

def check_repeat_identifiers(*args, **kwargs) -> pd.DataFrame:
    
    def _build_cte_query_from_column(colname):
        q = \
            f"flag_{colname} AS (SELECT GROUP_CONCAT(mpi, ',') AS mpi, '{colname}' AS field, {colname} AS value \n\
                FROM (SELECT DISTINCT mpi, {colname} FROM mpi_vectors) \n\
                    WHERE {colname} IS NOT NULL \n\
                        GROUP BY {colname} HAVING COUNT(mpi) > 1)"
                                        
        return q

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
            postlog.warn('Could not find any available columns to run check in MPI Vectors')
            return []
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




## Assemble known flags
Rule2 = Flag(
    checkfn = check_repeat_identifiers,
    report = Report(
        report_fn=simple_count
    ),
    write=None,
)
