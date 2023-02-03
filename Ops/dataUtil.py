import os
from os import environ
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime, timedelta
import pytz
import time

dbconn = None

def nowbyTZ(tzName):
    tformats = '%Y-%m-%d %H:%M:%S'
    tzinfo = time.tzname
    tNow = datetime.now()
    logging.info(f'Local System TimeZone info: {tzinfo[0]} and local time: {tNow}')

    targetNow = datetime.strptime(tNow.astimezone(pytz.timezone(tzName)).strftime(tformats), tformats)
    logging.info(f'Target TimeZone {tzName}:   Target time: {targetNow}')
    return targetNow

def get_Symbollist(listname):
    basedir = environ.get("PROD_LIST_DIR")
    listpath = os.path.join(basedir, f'{listname}.csv')
    s_list = pd.read_csv(listpath)['Symbol'].unique()
    return s_list

def get_DBengine():
    global dbconn
    if dbconn is None:
        hostname=environ.get("DBHOST")
        uname=environ.get("DBUSER")
        pwd=environ.get("DBPWD")
        DB = environ.get("DBMKTDATA")

        dbpath = "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=DB, user=uname, pw=pwd)
        logging.info(f'setup DBengine to {dbpath}')
        # Create SQLAlchemy engine to connect to MySQL Database
        dbconn = create_engine(dbpath)
        logging.debug(f'dbconn=>{dbconn}')
    return dbconn

def get_Max_date(dbntable, symbol=None):
    try:
        if symbol is None:
            query = f"SELECT max(Date) as maxdate from {dbntable} ;"
        else:
            query = f"SELECT max(Date) as maxdate from {dbntable} where symbol = \'{symbol}\';"
        logging.info(f'get_Max_date :{query}')
        df = pd.read_sql(query, get_DBengine())
        max_date = df.maxdate.iloc[0]
        logging.info(f"get_Max_date() => {max_date} ")
        return max_date
    except Exception as e:
        logging.error("Exception occurred at get_Max_date()", exc_info=True)

def get_Max_Options_date(dbntable, symbol=None):
    try:
        if symbol is None:
            query = f'SELECT max(Date) as maxdate, section FROM {dbntable} group by section order by maxdate desc;'
        else:
            query = f'SELECT max(Date) as maxdate, section FROM {dbntable} where UnderlyingSymbol = \'{symbol}\' group by section order by maxdate desc;'

        logging.info(f'get_Max_date :{query}')
        df = pd.read_sql(query, get_DBengine())
        logging.debug(df.head())
        max_date = df.maxdate.iloc[0]
        section = df.section.iloc[0]
        logging.info(f"get_Max_date() => {max_date},{section} ")
        return max_date, section
    except Exception as e:
        logging.error("Exception occurred at get_Max_Options_date()", exc_info=True)

def StoreEOD(eoddata, DBn, TBLn):
    try:
        logging.info(f'StoreEOD size: {len(eoddata)} in table:{TBLn} on DB:{DBn}')
        dbcon = get_DBengine()
        logging.info(f'StoreEOD dbcon: {dbcon}')
        # Convert dataframe to sql table
        eoddata.to_sql(name=TBLn, con=dbcon, if_exists='append', index=False)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

