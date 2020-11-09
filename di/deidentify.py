from assets.mapping import colmap

import logging 

dilogger = logging.getLogger(__name__)

def simple_di(data):
    dilogger.debug("DI Process Starting")
    std_columns = [col.lower() for col in colmap]
    mapped_columns = [col for col in data if col.lower() in std_columns and col != 'guid']
    dilogger.debug(f"Standard Columns Found:\n{std_columns}\nMapped Columns:\n{mapped_columns}")
    return data.drop(mapped_columns, axis=1)

