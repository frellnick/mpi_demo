# standardize.py

from .view import View
from assets.mapping import colmap
from .standards import pandas_standards_registry

import pandas as pd

import logging
stdlog = logging.getLogger(__name__)



def standardize(view):
    view, std_registry = _setup_standardization(view)
    args = [(col, view, std_registry) for col in view.columns]
    std_cols = list(map(standardize_col, args))
    col_dict = _expand_to_dict(std_cols)
    view.update(col_dict)
    return view


def _setup_standardization(view):
    if type(view) == pd.DataFrame:
        return View(view), pandas_standards_registry
    elif type(view) == View:
        return View 
    raise NotImplementedError(f'Cannot process data of type {type(view)}')


def standardize_col(*args, **kwargs):
    if len(args) == 1:
        if type(args) == tuple:
            colname, view, registry = args[0]
    
    stdlog.debug(f"Standardizing column {colname}")
    try:
        alias = _lookup_alias(colname)
        if alias is not None:
            return {colname: registry[alias](view, colname)}
        return None
    except Exception as e:
        raise e
    

def _expand_to_dict(ldicts: list) -> dict:
    t = {}
    for d in ldicts:
        if d is not None:
            t.update(d)
    return t


def _lookup_alias(colname, colmap=colmap):
    def _is_alias(colname):
        return colname in colmap.values()

    if colname in colmap:
        return colmap[colname]
    elif _is_alias(colname):
        return colname

    return None


