import pandas as pd 

import logging

mutillogger = logging.getLogger(__name__)

def compare_in(colname, searchlist):
    cond1 = colname in searchlist
    cond2 = '_'.join(colname.split('_')[1:]) in searchlist
    cond3 = sum([colname in item for item in searchlist]) > 0
    return sum([cond1, cond2, cond3]) > 0


def union_frames(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([t1, t2], ignore_index=True)


def get_column_intersect(dfa: pd.DataFrame, dfb: pd.DataFrame) -> list:
    c1 = set(dfa.columns.to_list())
    c2 = set(dfb.columns.to_list())
    return list(c1.intersection(c2))


def match_dataframe_columns(t1: pd.DataFrame, t2: pd.DataFrame) -> tuple:
    keep = get_column_intersect(t1, t2)
    return t1[keep], t2[keep]