#!/usr/bin/env python3
import argparse
from argparse import RawTextHelpFormatter
from datetime import datetime, timedelta

parser = argparse.ArgumentParser(description="""
print offshore insitu observations

Usage:
./printobs.py
""", formatter_class=RawTextHelpFormatter)
parser.add_argument("-sd",
                    metavar='startdate',
                    help="start date of time period to be downloaded")
parser.add_argument("-ed",
                    metavar='enddate',
                    help="end date of time period to be downloaded")
parser.add_argument("-s",
                    metavar='station',
                    help="station")
parser.add_argument("-i",
                    metavar='instrument',
                    help="instrument")

args = parser.parse_args()

if args.ed is None:
    args.ed = datetime.now() + timedelta(hours=3)

if args.sd is None:
    args.sd = args.ed - timedelta(hours=6)

if args.s is None:
    args.s = 'valhall'

if args.i is None:
    args.i = None

# -------------------------------------------------------------------- #
from utils import get_all_ts, print_formatted

varstr_lst = [
        'air_temperature',
        'wind_speed',
        'wind_from_direction',
        'sea_surface_wave_significant_height',
        'sea_surface_wave_mean_period',
        'sea_surface_wave_period_at_variance_spectral_density_maximum'
        ]

df = get_all_ts(args.sd,args.ed,args.s,varstr_lst)
print_formatted(df)
