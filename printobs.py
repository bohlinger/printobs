#!/usr/bin/env python3
import argparse
from argparse import RawTextHelpFormatter
from datetime import datetime, timedelta
import time
from utils import parse_date, call_frost_api, get_frost_df, print_formatted


parser = argparse.ArgumentParser(description="""
print offshore insitu observations

Usage:
./printobs.py
""", formatter_class=RawTextHelpFormatter)
parser.add_argument("-sd", metavar='startdate',
                    help="start date of time period to be downloaded")
parser.add_argument("-ed", metavar='enddate',
                    help="end date of time period to be downloaded")
parser.add_argument("-d", type=int, metavar='delta',
                    help="substracted hours from now (in lieu of sd & ed)")
parser.add_argument("-s", metavar='station', help="station")
parser.add_argument("-i", metavar='instrument',
                    help="instrument")

args = parser.parse_args()
dargs = vars(args)

# remove all None entries
dargs = {k: v for k, v in dargs.items() if v is not None}

ed = parse_date(dargs.get('ed',datetime.now() + timedelta(hours=3)))
sd = parse_date(dargs.get('sd',datetime.now() - timedelta(hours=6)))

if args.d is not None:
    sd = parse_date(args.ed) - timedelta(hours=args.d)

s = dargs.get('s','valhall')
i = None

# -------------------------------------------------------------------- #
varstr_dict= {
        'air_temperature':'    Ta',
        'wind_speed':'    FF',
        'wind_from_direction':'    DD',
        'sea_surface_wave_significant_height':'    Hs',
        'sea_surface_wave_mean_period':'  Tm02',
        'sea_surface_wave_period_at_variance_spectral_density_maximum':'    Tp'
        }
t1 = time.time()
r = call_frost_api(sd,ed,s,varstr_dict)
t2 = time.time()
print('time used for api call:', f'{t2-t1:.2f}', 'seconds')
df = get_frost_df(r,varstr_dict)
print_formatted(df,s)
t3 = time.time()
print('time used:', f'{t3-t1:.2f}', 'seconds')
