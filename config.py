# config.py

from decouple import config

import logging


DEBUG = config('DEBUG', default=True)
logfile = config('LOGFILE', default=None)

DATABASES = {
    'default': config(
        'DATABASE_URI',
        default='sqlite:///test.sqlite3'
    )
}


if DEBUG:
    loglevel = 'DEBUG'
else:
    loglevel = 'INFO'


def log_setup(loglevel, logfile):
    if loglevel == 'DEBUG':
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(filename=logfile, level=level)

log_setup(loglevel, logfile)
            


    

