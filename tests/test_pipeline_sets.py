# test_pipeline_sets.py

from db import get_mongo, get_session
from main import run_mpi

from random import choice
from itertools import permutations
import json

from .global_test_setup import testlogger



def cleanup_and_report(name:str=None) -> dict:
    report = {}
    report['name'] = name
    
    mg = get_mongo()
    res = mg.raw.delete_many({})
    mg_deleted = res.deleted_count
    report['count_documents'] = mg_deleted
    
    with get_session() as session:
        count_vectors = session.execute(
            'SELECT COUNT(*) FROM mpi_vectors'
        ).fetchone()
        session.execute(
            'DELETE FROM mpi_vectors WHERE 1=1;'
        )
        session.commit()
    report['count_vectors'] = count_vectors.values()[0]
    
    return report
    
    
    
def run_set(tablenames:list) -> dict:
    rundata = {}
    for tablename in tablenames:
        rundata[tablename] = run_mpi(tablename=tablename)
    
    report = cleanup_and_report(name='-'.join(tablenames))
    report['rundata'] = rundata
    return report



def run_and_clip_report(
        tablenames, keep=['name', 'count_documents', 'count_vectors']):
    report = run_set(tablenames)
    clipped = {}
    for col in keep:
        clipped[col] = report[col]
    return clipped



def test_ordered_ingest(datasets:list=None, output='test_report.json'):
    if datasets is None:
        # datasets = ['dws_wages', 'usbe_students', 'ushe_students', 'ustc_students']
        datasets = ['ustc_students', 'usbe_students', 'ushe_students']
    perms = [x for x in permutations(datasets, len(datasets))]

    reports = []
    for runorder in perms:
        try:
            reports.append(
                run_and_clip_report(runorder)
            )
        except Exception as e:
            reports.append({
                'name': '-'.join(runorder),
                'error': f"Run Failed. {e}",
            })
            testlogger.error(f'{e}', __name__)
    
    if output:
        with open(output, 'w+') as file:
            json.dump(reports, file, indent=2)