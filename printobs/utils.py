from datetime import datetime
import os
from dateutil.parser import parse
import pandas as pd
import yaml
import dotenv
import requests
import numpy as np
from pkg_resources import resource_stream

def load_yaml(name):
    return yaml.safe_load(
            resource_stream(__name__,name))

varstr_dict = load_yaml('variable_def.yaml')
insitu_dict = load_yaml('insitu_locations.yaml')

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

def call_frost_api(sdate, edate, nID, v):
    varstr_lst = list(varstr_dict.keys())
    varstr = ','.join(varstr_lst)
    dotenv.load_dotenv()
    client_id = os.getenv('CLIENT_ID', None)
    frost_reference_time = make_frost_reference_time_period(sdate, edate)
    if client_id is None:
        print("No Frost CLIENT_ID given!")
    if v == 'v0':
        r = call_frost_api_v0(nID, varstr,
                                frost_reference_time,
                                client_id)
    elif v == 'v1':
        r = call_frost_api_v1(nID, varstr,
                                frost_reference_time,
                                client_id)
    if r.status_code == 200:
        return r
    else:
        print(r.json()['error'])

def call_frost_api_v0(nID, varstr, frost_reference_time, client_id):
    ID = 'SN' + str(insitu_dict[nID]['ID'])
    endpoint = 'https://frost.met.no/observations/v0.jsonld' # v0
    parameters = {
                'sources': ID,
                'elements': varstr,
                'referencetime': frost_reference_time,
                'timeoffsets': 'default',
                'levels': 'default'
                }
    return requests.get(endpoint, parameters, auth=(client_id, client_id))

def call_frost_api_v1(nID, varstr, frost_reference_time, client_id):
    ID = insitu_dict[nID]['ID']
    endpoint = 'https://frost-prod.met.no/api/v1/obs/met.no/filter/get?'
    #endpoint = 'https://frost-prod.met.no/api/v1/obs/met.no/kvkafka/get?'
    parameters = {
                'stationids': ID,
                'elementids': varstr,
                'time': frost_reference_time,
                #'timeoffsets': 'default', # handled by filter
                'levels': 0,
                'incobs': 'true'
                }
    return requests.get(endpoint, parameters, auth=(client_id, client_id))

def get_frost_df(r,v):
    if v == 'v0':
        return get_frost_df_v0(r)
    elif v == 'v1':
        return get_frost_df_v1(r)

def get_frost_df_v0(r):
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

def get_frost_df_v1(r):
    # base df
    df = pd.json_normalize(r.json()['data']['tseries'])
    # df to be concatenated initialized with time
    # dfc3 = pd.json_normalize(r.json()['data']['tseries'])['header.extra.station.latitude'] # for stationary
    # dfc3 = pd.json_normalize(r.json()['data']['tseries'][0]['observations'][0]['body'])['lat'] # for moving platform
    dfc = pd.json_normalize(r.json()
      ['data']['tseries'][0]['observations'])['time'].to_frame()
    for vn in varstr_dict:
        idx = df['header.extra.element.id'][df['header.extra.element.id']==vn].index.to_list()
        for i in idx:
            dftmp = pd.json_normalize(r.json()\
                    ['data']['tseries'][i]['observations'])\
                    ['body.data'].to_frame()
            vns = varstr_dict[vn] + '_' + str(df['header.id.sensor'][i])
            dftmp = dftmp.rename(columns={ dftmp.columns[0]: vns })
            dftmp[vns] = dftmp[vns].mask(dftmp[vns] < 0, np.nan)
            dfc = pd.concat([dfc, dftmp.reindex(dfc.index)], axis=1)
    return dfc

flatten = lambda l: [item for sublist in l for item in sublist]

def sort_df(df):
    # get list of aliases
    alst = []
    for vn in varstr_dict:
        alst.append(varstr_dict[vn])
    # get list of df element keys
    elst = list(df.keys())
    nelst = []
    nelst.append(['time'])
    for va in alst:
        tmp = [elst[i] for i in range(len(elst)) if va in elst[i]]
        tmp.sort()
        nelst.append(tmp)
    # reorganize df according to sorted keys and return
    nelst = flatten(nelst)
    return df[nelst]

def print_formatted(df, nID):
    df = df.rename(columns={ df.columns[0]: '' })
    # quick and irty formatting
    # - need a smarter and more generic formatter
    dfstr = df.to_string(
        formatters={
                    "Hs_0": "{:,.1f}".format,
                    "Hs_1": "{:,.1f}".format,
                    "Hs_2": "{:,.1f}".format,
                    "Hs_3": "{:,.1f}".format,
                    "Hs_4": "{:,.1f}".format,
                    #
                    "Tm02_0": "{:,.1f}".format,
                    "Tm02_1": "{:,.1f}".format,
                    "Tm02_2": "{:,.1f}".format,
                    "Tm02_3": "{:,.1f}".format,
                    "Tm02_4": "{:,.1f}".format,
                    #
                    "Tp_0": "{:,.1f}".format,
                    "Tp_1": "{:,.1f}".format,
                    "Tp_2": "{:,.1f}".format,
                    "Tp_3": "{:,.1f}".format,
                    "Tp_4": "{:,.1f}".format,
                    #
                    "FF_0": "{:,.1f}".format,
                    "FF_1": "{:,.1f}".format,
                    "FF_2": "{:,.1f}".format,
                    "FF_3": "{:,.1f}".format,
                    "FF_4": "{:,.1f}".format,
                    #
                    "DD_0": "{:,.0f}".format,
                    "DD_1": "{:,.0f}".format,
                    "DD_2": "{:,.0f}".format,
                    "DD_3": "{:,.0f}".format,
                    "DD_4": "{:,.0f}".format,
                    #
                    "Ta_0": "{:,.1f}".format,
                    "Ta_1": "{:,.1f}".format,
                    "Ta_2": "{:,.1f}".format,
                    "Ta_3": "{:,.1f}".format,
                    "Ta_4": "{:,.1f}".format,
                    #
                    '': lambda x: "{:%Y-%m-%d %H:%M UTC }".format(pd.to_datetime(x, unit="ns"))
                    },
        index = False).split('\n')
    print('\n'.join(dfstr[0:1]))
    print('\n'.join(dfstr[1:]))
    print('\n'.join(dfstr[0:1]))
    print('--> ', nID, ' <--')

def print_available_locations():
    l = list(range(1,len(insitu_dict.keys())+1))
    dfc = pd.DataFrame(l)
    dfc = dfc.rename(columns={ dfc.columns[0]: '' })
    df = pd.DataFrame(insitu_dict.keys())
    df = df.rename(columns={ df.columns[0]: 'available locations' })
    dfc = pd.concat([dfc, df.reindex(dfc.index)], axis=1)
    dfstr = dfc.to_string(index=False).split('\n')
    print('----------------------')
    print('\n'.join(dfstr[0:1]))
    print('----------------------')
    print('\n'.join(dfstr[1:]))
    print('----------------------')
    print('Info:')
    print('above shown location aliases can be customized in insitu_locations.yaml')
    print('----------------------')
