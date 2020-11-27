# test_exceptions.py


from utils.exceptions import Bypass


def test_bypass():
    try: 
        raise Bypass
    except Bypass:
        res = True 
    
    assert res == True
