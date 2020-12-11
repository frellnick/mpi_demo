# test_util_filters.py


from utils.filters import search_list
from assets.mapping import blocked_identifiers

from .global_test_setup import testlogger

example_cols = [
    'student_id',
    'ssn',
    'first_name',
    'usbe_student_id',
    ]


def test_search_list():
    res = []
    for col in example_cols:
        if search_list(col, blocked_identifiers):
            res.append(col)
    assert len(res) == 3