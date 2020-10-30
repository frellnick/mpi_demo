#main.py

from utils.db import get_db
from config import *

from ingest import load_file
from mpi import create_distinct_view

db = get_db()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Ingest & MPI systems demo')

    parser.add_argument('--ingest')
    parser.add_argument('--tablename')

    args = parser.parse_args()

    if args.ingest is not None:
        if args.tablename is not None:
            load_file(filename=args.ingest, tablename=args.tablename)
        else:
            raise ValueError('Ingest requires a --tablename attribute.  Use --ingest <filename> --tablename <tablename>')


    print(create_distinct_view('ushe_students'))