# pandas standards.py


from utils import Registry
from assets.mapping import colmap
from mpi.prepare.view import View


pandas_standards_registry = Registry('pandas_standards')


# Standardization Functions

def ssn_pool(view:View):
    return 0
pandas_standards_registry.register(ssn_pool)


def guid(view:View):
    return 0
pandas_standards_registry.register(guid)


def last_name_pool(view:View):
    return 0
pandas_standards_registry.register(last_name_pool)


def first_name_pool(view:View):
    return 0
pandas_standards_registry.register(first_name_pool)


def middle_name_pool(view:View):
    return 0
pandas_standards_registry.register(middle_name_pool)


def student_id_pool(view:View):
    return 0
pandas_standards_registry.register(student_id_pool)


def ssid_pool(view:View):
    return 0 
pandas_standards_registry.register(ssid_pool)


def gender_pool(view:View):
    return 0 
pandas_standards_registry.register(gender_pool)


def birth_date_pool(view:View):
    return 0 
pandas_standards_registry.register(birth_date_pool)



# Validate Registry against Colmap

def validate_registry(registry: Registry, mapping = colmap) -> bool:
    for val in mapping.values():
        assert val in registry.keys(), f'{val} missing from registry {registry.name}'

validate_registry(pandas_standards_registry)