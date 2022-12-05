from datetime import datetime
import os
from dateutil.parser import parse
import pandas as pd
import yaml
import dotenv
import requests
import numpy as np
from pkg_resources import resource_stream
import xarray as xr

def load_yaml(name):
    return yaml.safe_load(
            resource_stream(__name__,name))

varstr_dict = load_yaml('variable_def.yaml')
insitu_dict = load_yaml('insitu_locations.yaml')

#def func(arg: arg_type, optarg: arg_type = default) -> return_type:

def parse_date(indate: str) -> datetime:
    """
    parsing input date
    """
    if isinstance(indate, datetime):
        return indate
    elif isinstance(indate, str):
        return parse(indate)
    else:
        print('Not able to parse input return as is')
        return indate

def make_frost_reference_time_period(\
    sdate: datetime, edate: datetime) -> str:
    """
    create special time format for frost call
    """
    sdate = parse_date(sdate)
    edate = parse_date(edate)
    formatstr = '%Y-%m-%dT%H:%M:00.000Z'
    refstr = '{}/{}'.format(sdate.strftime(formatstr),
                            edate.strftime(formatstr))
    return refstr

def call_frost_api(\
    sdate: datetime, edate: datetime,\
    nID: str, v: str) -> 'requests.models.Response':
    """
    make frost api call
    """
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

def call_frost_api_v0(\
    nID: str, varstr: str,frost_reference_time: str, client_id: str)\
    -> 'requests.models.Response':
    """
    frost call, retrieve data from frost v0
    """
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

def call_frost_api_v1(\
    nID: str, varstr: str,frost_reference_time: str, client_id: str)\
    -> 'requests.models.Response':
    """
    frost call, retrieve data from frost v1
    """
    ID = insitu_dict[nID]['ID']
    #endpoint = 'https://frost-prod.met.no/api/v1/obs/met.no/filter/get?'
    #endpoint = 'https://frost-prod.met.no/api/v1/obs/met.no/kvkafka/get?'
    endpoint = 'https://frost-beta.met.no/api/v1/obs/met.no/kvkafka/get?'
    #endpoint = 'https://frost-beta.met.no/api/v1/obs/met.no/filter/get?'
    parameters = {
                'stationids': ID,
                'elementids': varstr,
                'time': frost_reference_time,
                #'timeoffsets': 'default', # handled by filter
                'levels': 0,
                'incobs': 'true',
                'typeids': '22,11,510'
                }
    return requests.get(endpoint, parameters, auth=(client_id, client_id))

def get_frost_df(r: 'requests.models.Response',v: str)\
    -> 'pandas.core.frame.DataFrame':
    """
    retrieve frost data as pandas dataframe
    """
    if v == 'v0':
        return get_frost_df_v0(r)
    elif v == 'v1':
        return get_frost_df_v1(r)

def get_frost_df_v0(r: 'requests.models.Response')\
    -> 'pandas.core.frame.DataFrame':
    """
    create pandas dataframe from frost call for v0
    """
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
    # rename referenceTime to time
    df2 = df2.rename(columns={ 'referenceTime': 'time' })
    return df2

def get_frost_df_v1(r: 'requests.models.Response')\
    -> 'pandas.core.frame.DataFrame':
    """
    create pandas dataframe from frost call for v1
    """
    # empy sensor id lst
    sensor_id_lst = []
    # base df
    df = pd.json_normalize(r.json()['data']['tseries'])
    # df to be concatenated initialized with time
    # dfc3 = pd.json_normalize(r.json()['data']['tseries'])['header.extra.station.latitude'] # for stationary
    # dfc3 = pd.json_normalize(r.json()['data']['tseries'][0]['observations'][0]['body'])['lat'] # for moving platform
    dfc = pd.json_normalize(r.json()
      ['data']['tseries'][0]['observations'])['time'].to_frame()
    for vn in varstr_dict:
        idx = df['header.extra.element.id']\
                [df['header.extra.element.id']==vn].index.to_list()
        indices = df['header.id.sensor'][idx].values
        if len(indices) != len(np.unique(indices)):
            print("Caution:")
            print("-> sensor.id was not unique -> automatic selection")
            print("   affected ariable: ", vn)
            dflst = [] # list of dataframes
            nonlst = [] # list of numer of NaNs (non)
            for i in idx:
                dftmp = pd.json_normalize(r.json()\
                        ['data']['tseries'][i]['observations'])\
                        ['body.value'].to_frame()
                        #['body.data'].to_frame()
                vns = varstr_dict[vn] + '_' + str(df['header.id.sensor'][i])
                dftmp = dftmp.rename(columns={ dftmp.columns[0]: vns }).\
                            astype(float)
                dftmp[vns] = dftmp[vns].mask(dftmp[vns] < 0, np.nan)
                dflst.append(dftmp)
                nonlst.append(len(dftmp[np.isnan(dftmp)]))
            # check which df has least NaN and pick this one
            max_idx = nonlst.index(np.max(nonlst))
            dfc = pd.concat([dfc, dflst[max_idx].reindex(dfc.index)], axis=1)
        else:
            for i in idx:
                dftmp = pd.json_normalize(r.json()\
                        ['data']['tseries'][i]['observations'])\
                        ['body.value'].to_frame()
                        #['body.data'].to_frame()
                vns = varstr_dict[vn] + '_' + str(df['header.id.sensor'][i])
                dftmp = dftmp.rename(columns={ dftmp.columns[0]: vns }).\
                            astype(float)
                dftmp[vns] = dftmp[vns].mask(dftmp[vns] < 0, np.nan)
                dfc = pd.concat([dfc, dftmp.reindex(dfc.index)], axis=1)
    return dfc

def get_frost_df_info(r: 'requests.models.Response')\
    -> 'pandas.core.frame.DataFrame':
    df = pd.json_normalize(r.json()['data']['tseries'])
    dfc = df[['header.extra.timeseries.geometry.level.value']]
    dfc = dfc.rename(\
            columns={\
            'header.extra.timeseries.geometry.level.value':\
            'HAMSL [m] (repr)' }) # repr for represented height
    # add also values for actual height when available
    return dfc

def get_element_id_order(r: 'requests.models.Response')\
    -> list:
    df = pd.json_normalize(r.json()['data']['tseries'])
    idx_dict = {}
    idx_lst = []
    for vn in varstr_dict:
        idx = df['header.extra.element.id']\
                [df['header.extra.element.id']==vn].index.to_list()
        idx_dict[vn] = idx
        idx_lst.append(idx)
    return idx_dict, flatten(idx_lst)

flatten = lambda l: [item for sublist in l for item in sublist]

def sort_df(df: 'pandas.core.frame.DataFrame')\
    -> 'pandas.core.frame.DataFrame':
    """
    sort dataframe according to instrument number and rename accordingly
    """
    # get list of aliases
    alst = []
    for vn in varstr_dict:
        alst.append(varstr_dict[vn])
    # get list of df element keys and exclude other than str
    elst = [e for e in list(df.keys()) if isinstance(e,str)]
    # make sure that elst includes only strings
    nelst = []
    nelst.append(['time'])
    for va in alst:
        tmp = [elst[i] for i in range(len(elst)) if va in elst[i]]
        tmp.sort()
        nelst.append(tmp)
    # reorganize df according to sorted keys and return
    nelst = flatten(nelst)
    return df[nelst]

def format_df(df: 'pandas.core.frame.DataFrame')\
    -> list:
    """
    format data dataframe
    """
    df = df.rename(columns={ df.columns[0]: '' })
    # quick and dirty formatting
    # - need a smarter and more generic formatter
    dfstr = df.to_string(
        formatters = {
                    #"Hs_0": "{:,.1f}".format,
                    "Hs_0": "{:7.1f}".format,
                    "Hs_1": "{:,.1f}".format,
                    "Hs_2": "{:,.1f}".format,
                    "Hs_3": "{:,.1f}".format,
                    "Hs_4": "{:,.1f}".format,
                    "Hs_5": "{:,.1f}".format,
                    #
                    "Tm02_0": "{:7.1f}".format,
                    "Tm02_1": "{:,.1f}".format,
                    "Tm02_2": "{:,.1f}".format,
                    "Tm02_3": "{:,.1f}".format,
                    "Tm02_4": "{:,.1f}".format,
                    "Tm02_5": "{:,.1f}".format,
                    #
                    "Tp_0": "{:7.1f}".format,
                    "Tp_1": "{:,.1f}".format,
                    "Tp_2": "{:,.1f}".format,
                    "Tp_3": "{:,.1f}".format,
                    "Tp_4": "{:,.1f}".format,
                    "Tp_5": "{:,.1f}".format,
                    #
                    "FF_0": "{:7.1f}".format,
                    "FF_1": "{:,.1f}".format,
                    "FF_2": "{:,.1f}".format,
                    "FF_3": "{:,.1f}".format,
                    "FF_4": "{:,.1f}".format,
                    "FF_5": "{:,.1f}".format,
                    #
                    "DD_0": "{:7.0f}".format,
                    "DD_1": "{:,.0f}".format,
                    "DD_2": "{:,.0f}".format,
                    "DD_3": "{:,.0f}".format,
                    "DD_4": "{:,.0f}".format,
                    "DD_5": "{:,.0f}".format,
                    #
                    "Ta_0": "{:7.1f}".format,
                    "Ta_1": "{:,.1f}".format,
                    "Ta_2": "{:,.1f}".format,
                    "Ta_3": "{:,.1f}".format,
                    "Ta_4": "{:,.1f}".format,
                    "Ta_5": "{:,.1f}".format,
                    #
                    '': lambda x: "{:%Y-%m-%d %H:%M UTC }".format(pd.to_datetime(x, unit="ns"))
                    },
        index = False).split('\n')
    return dfstr

def format_info_df(
    df: 'pandas.core.frame.DataFrame',
    fdf:list,
    df_info: 'pandas.core.frame.DataFrame',
    info_lst:list,
    )\
    -> str:
    """
    format data dataframe of extra info
    """
    fstr = fdf[0]
    klst = list(df.keys())
    klst.remove('time')
    for n,key in enumerate(klst):
        idx = fstr.index(key)
        if n == 0:
            rstr = idx * " " + key
            vstr = info_lst[0]\
                    + (idx-len(info_lst[0])) * " "\
                    + len(key) * " "
        else:
            rstr += (idx-len(rstr))* " " + key
            val = df_info[key].values[0]
            if np.isnan(val) or val == 0:
                vstr += (idx-len(vstr))* " " + len(key)* " "
            else:
                #template = "{:" + str(len(key)) + ".1f}"
                template = "{:" + str(len(key)) + ".0f}"
                valstr = template.format(val)
                vstr += (idx-len(vstr))* " " + valstr
    return vstr

def print_formatted(dfstr: list, dfstr_info: str = None):
    """
    print formatted output of retrieved dataframe to screen
    """
    print('')
    print('\n'.join(dfstr[0:1]))
    print('\n'.join(dfstr[1:]))
    print('\n'.join(dfstr[0:1]))
    print('')
    if dfstr_info is not None:
        print(dfstr_info)
    print('')

def print_info(r: 'requests.models.Response',nID: str = None):
    print('--> ', nID, ' <--')
    dfkeys = pd.json_normalize(\
             r.json()['data']['tseries'][0]['observations']).keys()
    if 'body.lat' in dfkeys:
        # print recent location if moving
        lat = pd.json_normalize(r.json()\
                    ['data']['tseries'][0]['observations'])\
                    ['body.lat'].to_frame()['body.lat'].values[-1]
        lon = pd.json_normalize(r.json()\
                    ['data']['tseries'][0]['observations'])\
                    ['body.lon'].to_frame()['body.lon'].values[-1]
        # type of lat/lon is str
        print( "Location (recent): " + lon + "E " + lat + "N" )
    else:
        # print location if static
        df = pd.json_normalize(r.json()['data']['tseries'])
        lon = float(\
            df['header.extra.station.location']\
            [0][0]\
            ['value']['longitude'])
        lat = float(\
            df['header.extra.station.location']\
            [0][0]\
            ['value']['latitude'])
        print(\
                "Location (i.e. sensor #0): {:.2f}E".format(lon) \
              + " {:.2f}N".format(lat) )

def get_info_df(
    r: 'requests.models.Response',
    df: 'pandas.core.frame.DataFrame',
    dfi: 'pandas.core.frame.DataFrame',
    ) -> 'pandas.core.frame.DataFrame':
    # get order
    _,idx_lst = get_element_id_order(r)
    # get variable df
    df = df.rename(columns={ df.columns[0]: '' })
    tmpdf = df[df['']==df[''][0]]
    # get df with additional info
    dfikeys = list(dfi.keys())
    dfi = dfi.transpose().reset_index()[idx_lst]
    dfi.insert(loc=0, column='time', value='')
    # rename '' to time for sorting
    tmpdf = tmpdf.rename(columns={ tmpdf.columns[0]: 'time' })
    # rename dfi columns according to df columns
    for idx, item in enumerate(tmpdf.keys()):
        dfi = dfi.rename(columns={ dfi.columns[idx]: item })
    tmpdf = sort_df(tmpdf)
    dfi = sort_df(dfi)
    dfi['time'] = tmpdf['time']
    dfc = pd.concat([tmpdf,dfi])
    dfc = dfc.reset_index()[1:2].drop(columns='index')
    for key in dfc.keys():
        #if key != 'index':
        if key != 'time':
            dfc[key]=dfc[key].astype(float)
    return dfc

def print_available_locations():
    """
    print available offshore locations
    """
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

def dump(df: 'pandas.core.frame.DataFrame', ptf: str, f: str):
    """
    write retrieved data to file
    """
    if f == 'nc':
        ds = df.to_xarray()
        ds.to_netcdf(ptf)
    elif f == 'p':
        df.to_pickle(ptf)
    elif f == 'csv':
        df.to_csv(ptf)
