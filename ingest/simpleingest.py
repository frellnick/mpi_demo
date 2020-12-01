#simpleingest.py

import logging
import pandas as pd
import time

from db import dataframe_to_db

logger = logging.getLogger(__name__)


def load_file(filename: str, tablename: str):
    def _gen_guid():
        return hash(time.time())

    logger.info(f"Loading file '{filename}' to '{tablename}'")
    df = pd.read_csv(filename, low_memory=False)

    guid = _gen_guid()
    logger.info(f'GUID assigned: {guid}')
    df['guid'] = guid
    dataframe_to_db(df, tablename=tablename)