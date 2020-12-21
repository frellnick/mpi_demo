# pandas standards.py


from utils import Registry
from mpi.prepare.view import View


pandas_standards_registry = Registry('pandas_standards')


# Standardization Functions

def ssn_pool(view:View):
    return view['ssn_pool']
pandas_standards_registry.register(ssn_pool)


def guid(view:View):
    return view['guid']
pandas_standards_registry.register(guid)