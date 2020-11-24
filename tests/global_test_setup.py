from config import logfile
import datetime


class FileLogger():
    def __init__(self, logfile):
        self.logfile = logfile
        self._write_to_file(f'Log Start {datetime.datetime.now()}')


    def _write_to_file(self, message):
        with open(logfile, 'a+') as f:
            timestamp = f'{datetime.datetime.now()}'
            f.writelines(f'{timestamp} {message} \n')

    def info(self, message):
        self._write_to_file(f'INFO: {message}')

    def debug(self, message):
        self._write_to_file(f'DEBUG: {message}')


testlogger = FileLogger(logfile=logfile)