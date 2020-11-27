"""
MPI Preprocessing

    Nondestructive cleaning and dataframe alignment.
    Ensures like fields have matching format, datatype.
    Remove extraneous characters from string fields
"""

import pandas as pd
from recordlinkage.preprocessing import clean
from utils import get_column_intersect

import logging

preplogger = logging.getLogger(__name__)



def clean_raw(df: pd.DataFrame, exclude: ['date', 'mpi']):
    def _is_excluded(col:str, exclude:list) -> bool:
        return True in [e in col for e in exclude]

    obj_columns = df.select_dtypes(include='object').columns
    temp = df.copy()
    for col in obj_columns:
        if not _is_excluded(col, exclude):
            temp[col] = clean(temp[col])
    return temp



def match_dtype(dview: pd.DataFrame, iview: pd.DataFrame):
    valid_columns = get_column_intersect(dview, iview)
    df1 = dview.copy()[valid_columns]
    valid_columns.append('mpi')
    df2 = iview.copy()[valid_columns]
    for row in df1.dtypes.iteritems():
        col = row[0]
        if col in df1.columns:
            try:
                df2[col] = df2[col].astype(row[1])
                preplogger.info(f"Column {col} attempting cast to {row[1]}")
            except ValueError:
                preplogger.info(f"Casting of ID Column {col} failed.  Attempting pd.to_numeric.")
                df2[col] = pd.to_numeric(df2[col])
                df1[col] = pd.to_numeric(df1[col].astype(float))
                preplogger.info(f"Data.Type {df1[col].dtype}; ID.Type {df2[col].dtype}")
            except Exception as e:
                preplogger.error(f'Could not cast columns {col}\n{e}')
    return df1, df2
