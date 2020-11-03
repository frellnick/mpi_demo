import pandas as pd 



def compare_in(colname, searchlist):
    cond1 = colname in searchlist
    cond2 = '_'.join(colname.split('_')[1:]) in searchlist
    return cond1 or cond2


def union_frames(t1: pd.DataFrame, t2: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([t1, t2], ignore_index=True)