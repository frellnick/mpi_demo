# pandas standards.py


from utils import Registry
from assets.mapping import colmap
from mpi.prepare.view import View


pandas_standards_registry = Registry('pandas_standards')


# Standardization Functions

def ssn_pool(view:View):
    def _test_ssn(x):
        try:
            tx = str(x)
            assert len(tx) == 9
            assert tx[0:3] != '000'
            assert tx[0:3] != '666'
            assert int(tx[0:3]) < 900
            assert int(tx[3:5]) > 0
            assert int(tx[-4:]) > 0
            return int(x)
        except:
            return None
    
    return view['ssn_pool'].apply(_test_ssn)
pandas_standards_registry.register(ssn_pool)


def guid(view:View):
    return view['guid']
pandas_standards_registry.register(guid)


def last_name_pool(view:View):
    return view['last_name_pool']
pandas_standards_registry.register(last_name_pool)


def first_name_pool(view:View):
    return view['first_name_pool']
pandas_standards_registry.register(first_name_pool)


def middle_name_pool(view:View):
    return view['middle_name_pool']
pandas_standards_registry.register(middle_name_pool)


def student_id_pool(view:View):
    return view['student_id_pool']
pandas_standards_registry.register(student_id_pool)


def ssid_pool(view:View):
    return view['ssid_pool']
pandas_standards_registry.register(ssid_pool)


def gender_pool(view:View):
    return view['gender_pool']
pandas_standards_registry.register(gender_pool)


def birth_date_pool(view:View):
    return view['birth_date_pool']
pandas_standards_registry.register(birth_date_pool)



## Common Utilities




# Validate Registry against Colmap

def validate_registry(registry: Registry, mapping = colmap) -> bool:
    for val in mapping.values():
        assert val in registry.keys(), f'{val} missing from registry {registry.name}'

validate_registry(pandas_standards_registry)