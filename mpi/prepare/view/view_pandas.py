# view_pandas.py


from utils import Registry
import pandas as pd
from assets.mapping import colmap, local_identifiers

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


def update(data: pd.DataFrame, col_dict: dict) -> pd.DataFrame:
    for key in col_dict:
        data[key] = col_dict[key]
    return data
df_registry.register(update)


def subset(data: pd.DataFrame, context: dict, colmap = colmap) -> pd.DataFrame:
    mc, rn = filter_mapped_columns(data.columns, context, colmap)
    s = data[mc].copy()
    s = s.drop_duplicates()
    s = s.rename(rn, axis=1)
    return s
df_registry.register(subset)


def merge(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    l = left.copy()
    mc, rn = filter_mapped_columns(left.columns)

    return pd.merge(l, r, how='left', left_on=std_columns, right_on=std_columns)
df_registry.register(merge)



## General Utilities ##
def filter_mapped_columns(table_columns: list, context: dict, colmap = colmap) -> list:
    def _rename_column(col, context: dict, colmap = colmap):
        pool_name = colmap[col]  # get standard name from mapping
        if pool_name in local_identifiers:
            return f"{context['partner']}_{pool_name}".lower() # Prepend partner name to base column name
        return pool_name

    mapped_columns = [col for col in table_columns if col in colmap.keys()]
    renaming = {}
    [renaming.update({col:_rename_column(col, context, colmap)}) for col in mapped_columns]
    return mapped_columns, renaming