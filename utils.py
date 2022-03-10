from datetime import datetime
import os
from dateutil.parser import parse
import pandas as pd
import yaml
import dotenv
import requests


with open('insitu_locations.yaml', 'r') as file:
    insitu_dict = yaml.safe_load(file)

def parse_date(indate):
    if isinstance(indate, datetime):
        return indate
    elif isinstance(indate, str):
        return parse(indate)
    else:
        print('Not able to parse input return as is')
        return indate

def make_frost_reference_time_period(sdate, edate):
    sdate = parse_date(sdate)
    edate = parse_date(edate)
    formatstr = '%Y-%m-%dT%H:%M:00.000Z'
    refstr = '{}/{}'.format(sdate.strftime(formatstr),
                            edate.strftime(formatstr))
    return refstr

def call_frost_api(sdate,edate,nID,varstr_dict):
    varstr_lst = list(varstr_dict.keys())
    varstr = ','.join(varstr_lst)
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
                'timeoffsets': 'default',
                'levels': 'default'
                }
    return requests.get(endpoint, parameters, auth=(client_id, client_id))

def get_frost_df(r,varstr_dict):
    varstr_lst = list(varstr_dict.keys())
    d = {}
    length = len(r.json()['data'])
    d['referenceTime'] = [r.json()['data'][t]['referenceTime'] for t in range(length)]
    for v,i in zip(varstr_lst,range(len(varstr_lst))):
        d[v] = [r.json()['data'][t]['observations'][i]['value']
                for t in range(length)]
    df = pd.DataFrame(d)
    varstr_dict['referenceTime'] = 'time'
    df = df.rename(columns=varstr_dict)
    return df

def print_formatted(df, nID):
    print('\n'.join(df.to_string(index = False).split('\n')[1:]))
    print('\n'.join(df.to_string(index = False).split('\n')[0:1]))
    print('--> ', nID, ' <--')
