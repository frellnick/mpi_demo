"""
View

Create a view of data that exposes necessary methods for downstream usage.

Defaults to underlying storage (dataframe or database session) where method not expressed if possible.
"""


import pandas as pd


class View:
    def __init__(self, data):
        if type(data) == pd.DataFrame:
            self.data = data
        else:
            raise NotImplementedError('Unable to initialize abstract database table view')