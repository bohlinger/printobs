#!/usr/bin/env python3
import argparse
from argparse import RawTextHelpFormatter
from datetime import datetime, timedelta
import time
from .utils import parse_date, call_frost_api
from .utils import get_frost_df, print_formatted
from .utils import print_available_locations
from .utils import sort_df

def main():
    parser = argparse.ArgumentParser(description="""
    print offshore insitu observations

    Usage:

    For a list of available stations:
    ./printobs

    Qyery a specific station:
    ./printobs -s draugen

    Extend query back in time (here e.g. 24h):
    ./printobs -s draugen -d 24

    Adding start and end date to query:
    ./printobs -s draugen -sd 20220401 -ed 20220404
    ./printobs -s draugen -sd 2022-04-01-12 -ed 2022.04.04-06:15

    """, formatter_class=RawTextHelpFormatter)
    parser.add_argument("-sd", metavar='startdate',
                        help="start date of time period to be downloaded")
    parser.add_argument("-ed", metavar='enddate',
                        help="end date of time period to be downloaded")
    parser.add_argument("-d", type=int, metavar='delta',
                        help="substracted hours from now (in lieu of sd & ed)")
    parser.add_argument("-s", metavar='station', help="station")
    parser.add_argument("-i", metavar='instrument', help="instrument")
    parser.add_argument("-v", metavar='version', help="FROST API version")

    args = parser.parse_args()
    dargs = vars(args)

# remove all None entries
    dargs = {k: v for k, v in dargs.items() if v is not None}

    ed = parse_date(dargs.get('ed',datetime.now() + timedelta(hours=3)))
    sd = parse_date(dargs.get('sd',datetime.now() - timedelta(hours=12)))

    if args.d is not None:
        sd = parse_date(ed) - timedelta(hours=args.d)

    s = dargs.get('s')
    i = None
    v = dargs.get('v','v1')

# -------------------------------------------------------------------- #
    if s is None:
        # print available locations
        print_available_locations()
    else:
        t1 = time.time()
        r = call_frost_api(sd,ed,s,v)
        t2 = time.time()
        print('time used for api call:', f'{t2-t1:.2f}', 'seconds')
        df = get_frost_df(r,v)
        # reorganize df
        df = sort_df(df)
        print_formatted(df,s)
        t3 = time.time()
        print('time used:', f'{t3-t1:.2f}', 'seconds')
