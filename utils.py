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
    alias_lst = [varstr_dict[e] for e in varstr_dict]
    df = pd.json_normalize(r.json()['data'],
                            ['observations'],
                            ['referenceTime'])
    df2 = df['referenceTime'].drop_duplicates().reset_index(drop=True)
    df2 = df2.to_frame()
    for v in varstr_lst:
        dftmp = df.loc[df['elementId'] == v]['value'].reset_index(drop=True).to_frame()
        dftmp = dftmp.rename(columns={ dftmp.columns[0]: varstr_dict[v] })
        df2 = pd.concat([df2, dftmp.reindex(df2.index)], axis=1)
    return df2

def print_formatted(df, nID):
    print('\n'.join(df.to_string(index = False).split('\n')[1:]))
    print('\n'.join(df.to_string(index = False).split('\n')[0:1]))
    print('--> ', nID, ' <--')
