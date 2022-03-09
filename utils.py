from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import os
import yaml

with open('insitu_specs.yaml', 'r') as file:
    insitu_dict = yaml.safe_load(file)

def parse_date(indate):
    if isinstance(indate,datetime):
        return indate
    elif isinstance(indate,str):
        return parse(indate)
    else:
        print('Not able to parse input return as is')
        return indate

def make_frost_reference_time_period(sdate,edate):
    sdate = parse_date(sdate)
    edate = parse_date(edate)
    formatstr = '%Y-%m-%dT%H:%M:00.000Z'
    refstr = '{}/{}'.format(sdate.strftime(formatstr),
                            edate.strftime(formatstr))
    return refstr

def get_frost_ts(sdate,edate,nID,varstr):
    import dotenv
    import requests
    import pandas as pd
    dotenv.load_dotenv()
    client_id = os.getenv('CLIENT_ID', None)
    if client_id is None:
        print("No Frost CLIENT_ID given!")
    ID = insitu_dict[nID]['ID']
    frost_reference_time = make_frost_reference_time_period(sdate,edate)
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    parameters = {
                'sources': ID,
                'elements': varstr,
                'referencetime': frost_reference_time,
                }
    r = requests.get(endpoint, parameters, auth=(client_id,client_id))
    df = pd.json_normalize( r.json()['data'],
                            ['observations'],
                            ['sourceId','referenceTime'])
    return df

def get_all_ts(sdate,edate,nID,varstr_lst):
    df = get_frost_ts(sdate,edate,nID,varstr_lst[0])['referenceTime']
    for varstr in varstr_lst:
        try:
            dftmp = get_frost_ts(sdate,edate,nID,varstr)['value']
            df = pd.concat([df, dftmp.reindex(df.index)], axis=1)
        except Exception as e:
            print(e)
            print(varstr,' not available')
            dftmp = pd.DataFrame([-999]*len(df))
    return df

def print_formatted(df,nID):
    print('\n'.join(df.to_string(index = False).split('\n')[1:]))
    print('time                       TEMP     FF     DD     Hs   Tm02     Tp')
    print(nID)
