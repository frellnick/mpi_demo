# view_pandas.py


from utils import Registry
import pandas as pd

df_registry = Registry('df_view')


## Access/View functions

def dfcolumns(data: pd.DataFrame) -> list:
    return data.columns.tolist()
df_registry.register(dfcolumns, name='columns')


def dflen(data: pd.DataFrame) -> int:
    return len(data)
df_registry.register(dflen, name='dlen')


def getitem(data: pd.DataFrame, idx) -> pd.Series:
    return data[idx]
df_registry.register(getitem)


def head(data: pd.DataFrame, nrows:int) -> pd.DataFrame:
    return data.head(nrows)
df_registry.register(head)