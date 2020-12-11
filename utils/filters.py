# filters.py

def search_list(sstr: str, slist: list, mtype: str = 'strict') -> bool:
    if mtype == 'strict':
        for v in [c.strip('_pool') for c in slist]:
            if v in sstr:
                return True 
        return False