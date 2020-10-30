#main.py

from utils.db import init_db
from config import *

from ingest import load_file
from mpi import create_distinct_view, create_identity_view, link


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Ingest & MPI systems demo')

    parser.add_argument('--ingest')
    parser.add_argument('--tablename')
    parser.add_argument('--initdb', action='store_true')

    args = parser.parse_args()

    print(args)

    # Initialize Database if necessary
    if args.initdb:
        init_db()


    if args.ingest is not None:
        if args.tablename is not None:
            load_file(filename=args.ingest, tablename=args.tablename)
        else:
            raise ValueError('Ingest requires a --tablename attribute.  Use --ingest <filename> --tablename <tablename>')

    

    dview = create_distinct_view('ushe_students')
    print(dview.head())

    iview = create_identity_view(dview.columns.tolist())
    print(iview)

    print(link(dview, iview))