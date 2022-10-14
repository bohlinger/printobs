import pytest
from datetime import datetime

def test_parse_date():
    from utils import parse_date
    assert datetime(2022,1,1) == parse_date("2022-1-1")

def test_call_frost_api():
    from utils import call_frost_api
    r = call_frost_api( sdate=datetime(2022,1,1),
                        edate=datetime(2022,1,2),
                        nID='draugen',v='v0')
    print(vars(r.status_code).keys())
    assert r.status_code == 200
