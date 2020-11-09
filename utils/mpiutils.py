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


def match_columns(t1: pd.DataFrame, t2: pd.DataFrame) -> tuple:
    col1 = set(list(t1.columns))
    col2 = set(list(t2.columns))
    keep = list(col1.intersection(col2))
    return t1[keep], t2[keep]