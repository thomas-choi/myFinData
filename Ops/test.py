from eoddata_fetch import get_Max_date
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
import time
import pytz
import pandas as pd
from os import environ

# import sys, getopt

import  dataUtil as DU

load_dotenv("../Prod_config/Stk_eodfetch_PythonAnywhere.env") #Check path for env variables
logging.getLogger().setLevel(logging.DEBUG)

def DBTest():

    mkdate = get_Max_date('twmchoi2022$GlobalMarketData.histdailyprice6')

    print(mkdate)

def TimeTest():
    tzinfo = time.tzname
    logging.info(f'TimeZone info: {tzinfo[0]}\n\n')

    tformats = '%Y-%m-%d %H:%M:%S'

    tNow = datetime.now()
    typeNow = type(tNow)
    logging.debug(f'Type of tNow:{typeNow}')
    today5PM = tNow.replace(hour=17, minute=0, second=0, microsecond=0)
    logging.info(f'Current Date-Time is {tNow}, and cutt-off time is {today5PM}')

    mToday = tNow.date()
    logging.info(f'mToday is {mToday}')
    if tNow < today5PM:
        mToday = mToday - timedelta(days=1)
    logging.info(f'mToday after cutoff is {mToday}\n\n')

    estNow = datetime.strptime(tNow.astimezone(pytz.timezone('US/Eastern')).strftime(tformats), tformats)
    logging.info(f'Current Date-Time is {estNow}')
    typeNow = type(estNow)
    logging.debug(f'Type of estNow:{typeNow}')
    today5PM = estNow.replace(hour=17, minute=0, second=0, microsecond=0)
    logging.info(f'     and cutt-off time is {today5PM}')

    estToday = estNow.date()
    logging.info(f'estToday is {estToday}')
    if estNow < today5PM:
        estToday = estToday - timedelta(days=1)
    logging.info(f'mToday after cutoff is {estToday}\n\n')

def ImportOptionChain(inFile):

    logging.info(f'Import File: {inFile} to database')
    opt_tbl=environ.get("TBLOPTCHAIN")
    all_chains = pd.read_csv(inFile)
    recnum = len(all_chains)
    logging.info(f'Record # {recnum}')
    all_chains['Date'] = pd.to_datetime(all_chains['Date'])
    all_chains['Expiration'] = pd.to_datetime(all_chains['Expiration'])

    logging.debug(f'data file: {all_chains.info()}')
    DU.StoreEOD(all_chains, " ", opt_tbl)

import argparse

load_dotenv("../Prod_config/Stk_eodfetch.env") #Check path for env variables

parser = argparse.ArgumentParser()
parser.add_argument('-S', '--Sect', dest='InSect', type=str)
parser.add_argument('-D', '--Date', dest='InDate', type=str)
parser.add_argument('-U', '--Upload', dest='upload', action='store_true', default=False)
parser.add_argument('-i', '--import', dest='InFile', default='', type=str)

args = parser.parse_args()

print(args)
print(args.InSect)
print(args.InDate)
if args.upload:
    print('Upload all files')
else:
    print("Don't upload files")

if len(args.InFile)> 0:
    ImportOptionChain(args.InFile)

if len(args.InDate)>0:
    todt = datetime.strptime(args.InDate, '%Y-%m-%d')
    print(type(todt), todt)
    wd = todt.date().isoweekday()
    wf = wd in range(1,6)
    print(f'Weekday of {todt}: {wd} is in (1-5) {wf}')

# my_datetime_est = datetime.today().astimezone(pytz.timezone('US/Eastern'))
# print(f'mToday in EST is {my_datetime_est}')

# my_datetime_est = datetime.today().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
# print(f'mToday in EST is {my_datetime_est}')

# my_datetime_est = datetime.today().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S %Z%z')
# print(f'mToday in EST is {my_datetime_est}')