# MPI Demo System
A mock system for testing MPI implementations

## Mock Ingest
Loads .csv into pandas and dumps to sqlite table in common database

## MPI
Exposes API for processing ingested tables

## DI
Exposes API to de-identify a table


## Tests

Tests written with pytest

Run all tests with:

```bash
XXXXX
```

### Managing Conda Environments

**Update** environment.yml with 

```bash
conda env export > environment.yml
```

* Check that pip dependencies written in.

### Creating PipEnv shell

You can create a pipenv shell from environment.yml via:

1. Steps (probably want a requirements.txt or something)