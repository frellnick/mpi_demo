# MPI Demo System
A mock system for testing MPI implementations

## Mock Ingest
Loads .csv into pandas and dumps to sqlite table in common database

## MPI
Exposes API for processing ingested tables

## DI
Exposes API to de-identify a table


### Starting Databases with Docker or Podman

MPI NoSQL demos require a configured MongoDB instance.

Setting up MongoDB
1. Start MongoDB container

> podman run -d --name mongo_db -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=mongoadmin -e MONGO_INITDB_ROOT_PASSWORD=dbpassword mongo

2. Enter the container and create a collection

> podman exec -it mongo_db bash

3. Create application Role and DB (or skip and use root stuff for testing)

> XXXX


## Tests

Tests written with pytest

Run all tests with:

```bash
pytest
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