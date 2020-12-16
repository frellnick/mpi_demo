# standardize.py

from .view import View

import pandas as pd

def standardize(view: View, std_registry:dict):  #TODO: export View instead of dataframe
    view = View(view)
    std_cols = list(map(standardize_col, view.columns, args=(view, std_registry)))


def standardize_col(colname, view, registry):
    pass