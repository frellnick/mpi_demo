"""
MPI Preprocessing

    Nondestructive cleaning and dataframe alignment.
    Ensures like fields have matching format, datatype.
    Remove extraneous characters from string fields
"""

import pandas as pd
from recordlinkage.preprocessing import clean

import logging

preplogger = logging.getLogger(__name__)

def clean_raw(df: pd.DataFrame):
    obj_columns = df.select_dtypes(include='object').columns
    temp = df.copy()
    for col in obj_columns:
        if 'date' not in col:
            temp[col] = clean(temp[col])
    return temp


def match_dtype(dfa: pd.DataFrame, dfb: pd.DataFrame):
    df1 = dfa.copy()
    df2 = dfb.copy()
    for row in df1.dtypes.iteritems():
        col = row[0]
        if col in df2.columns:
            try:
                df2[col] = df2[col].astype(row[1])
                preplogger.info(f"Column {col} attempting cast to {row[1]}")
            except ValueError:
                preplogger.info(f"Casting of ID Column {col} failed.  Attempting pd.to_numeric.")
                df2[col] = pd.to_numeric(df2[col])
                df1[col] = pd.to_numeric(df1[col].astype(float))
                preplogger.info(f"Data.Type {df1[col].dtype}; ID.Type {df2[col].dtype}")
    return df1, df2
