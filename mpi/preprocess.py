"""
MPI Preprocessing

    Nondestructive cleaning and dataframe alignment.
    Ensures like fields have matching format, datatype.
    Remove extraneous characters from string fields
"""

import pandas as pd
from recordlinkage.preprocessing import clean


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
            df2[col] = df2[col].astype(row[1])
    
    return df1, df2
