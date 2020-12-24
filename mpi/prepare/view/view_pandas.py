# view_pandas.py


from utils import Registry, get_column_intersect
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
    s = map_columns(data, context, colmap, trim=True)
    s = s.drop_duplicates()
    return s
df_registry.register(subset)


def merge(left: pd.DataFrame, right: pd.DataFrame, context: dict, colmap = colmap, map_left=True, map_right=False, keep=['mpi']) -> pd.DataFrame:
    if map_left:
        l = map_columns(left, context, colmap)
    else:
        l = left 
    if map_right:
        r = map_columns(right, context, colmap)
    else:
        r = right

    mcols = get_column_intersect(l, r)
    t = pd.merge(l, r, how='left', left_on=mcols, right_on=mcols)
    for k in keep:
        left[k] = t[k]
    return left


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


def map_columns(df: pd.DataFrame, context: dict, colmap: dict, trim=False) -> pd.DataFrame:
    mc, rn = filter_mapped_columns(df.columns, context, colmap)
    if trim:
        a = df[mc].copy()
    else:
        a = df.copy()
    a = a.rename(rn, axis=1)
    return a