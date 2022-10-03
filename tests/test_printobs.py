import pytest
from datetime import datetime

def test_parse_date():
    from printobs import parse_date
    assert datetime(2022,1,1) == parse_date("2022-1-1")

def test_call_frost_api():
    from printobs import call_frost_api_v1
    r = call_frost_api( sdate=datetime(2022,1,1),
                        edate=datetime(2022,1,2),
                        nID='draugen',v='v1')
    print(vars(r.status_code).keys())
    assert r.status_code == 200
