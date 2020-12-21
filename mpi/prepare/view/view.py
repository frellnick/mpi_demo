"""
View

Create a view of data that exposes necessary methods for downstream usage.

Defaults to underlying storage (dataframe or database session) where method not expressed if possible.
"""


import pandas as pd

from .view_pandas import df_registry

class View:
    def __init__(self, data):
        if type(data) == pd.DataFrame:
            self.data = data
            self.fn_registry = df_registry
        else:
            raise NotImplementedError('Unable to initialize abstract database table view')


    def __len__(self):
        return self.fn_registry['dlen'](self.data)

    
    def __getitem__(self, idx):
        return self.fn_registry['getitem'](self.data, idx)


    @property 
    def columns(self):
        return self.fn_registry['columns'](self.data)


    def head(self, nrows=5):
        return self.fn_registry['head'](self.data, nrows)


    def update(self, col_dict):
        """
        params
        col_dict: (dictionary) dictionary of columns, where they key is the name of the column in View
        """
        self.data = self.fn_registry['update'](self.data, col_dict)