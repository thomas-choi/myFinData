# python standard modules
import time
from datetime import datetime, timedelta
import os

# import numpy, pyplot and scipy
import numpy as np
import pandas as pd

from dotenv import load_dotenv
import logging
from os import environ
import yfinance as yf
import argparse

import dataUtil as DU

DATADIR = 'OptionsChain'
LOADDIR = 'loadDB'

def option_chains(ticker):
    """https://www.pythonanywhere.com/user/twmchoi2022/files/home/twmchoi2022/myFinData/Ops
    """
    asset = yf.Ticker(ticker)
    expirations = asset.options
    logging.debug(f'{ticker} option chain: {expirations}')
    chains = pd.DataFrame()

    for expiration in expirations:
        # tuple of two dataframes
        try:
            opt = asset.option_chain(expiration)

            calls = opt.calls
            calls['OptionType'] = "call"

            puts = opt.puts
            puts['OptionType'] = "put"

            chain = pd.concat([calls, puts])
            chain['Expiration'] = pd.to_datetime(expiration)

            chains = pd.concat([chains, chain])
        except Exception as e:
            logging.error(f"asset.option({ticker}.{expiration}) error: {e}")
    try:
        chains['UnderlyingSymbol'] = ticker
        histdata = asset.history()
        if len(histdata)>0:
            lastdata = asset.history().iloc[-1]
            chains['UnderlyingPrice'] = lastdata.Close
        else:
            logging.error(f"option_chains({ticker}.{expiration}) Underlying Price error")
            chains['UnderlyingPrice'] = -0.0001
    except Exception as e:
        logging.error(f"option_chains({ticker}.{expiration}) error: {e}")

    return chains

def pickTopOI(df, pickN=5):
    if len(df)>0:
        qstr = 'OptionType == \'put\' and strike < UnderlyingPrice'
        logging.debug(qstr)
        putdf = df.query(qstr).sort_values(by='openInterest', ascending=False)[:pickN]
        qstr = 'OptionType == \'call\' and strike > UnderlyingPrice'
        logging.debug(qstr)
        calldf = df.query(qstr).sort_values(by='openInterest', ascending=False)[:pickN]
        return pd.concat([putdf, calldf])
    else:
        return pd.DataFrame(columns=df.columns.values)

def ProcessOptions(ticker, process_dt, section, topOI, usedFile=False):
    logging.debug(f'ProcessOptions {ticker} on {process_dt}:{section}')
    underlying_symbol = ticker
    csvN = os.path.join(DATADIR, f'{underlying_symbol}_{process_dt}-{section}.csv')
    if os.path.exists(csvN) and usedFile:
        # read the original frame in from cache (pickle)
        logging.info(f'Recover data from {csvN}')
        options_frame = pd.read_csv(csvN)
    else:
        # define a Options object
        logging.info(f'Download {underlying_symbol} option chain')
        options_frame = option_chains(underlying_symbol)
        # let's pickle the dataframe so we don't have to hit the network every time
        if usedFile:
            options_frame.to_csv(csvN, index=False)
            logging.debug(f' Save file {csvN}')
    if len(options_frame) > 0:
        options_frame = options_frame.sort_values(by=['Expiration','OptionType'])
        options_frame['contractSize'] = 100
        options_frame.insert(0, "Section", section)
        options_frame.insert(0, "Date", process_dt)
        options_frame['inTheMoney'] = options_frame['inTheMoney'].astype('bool')
    OI_df = pickTopOI(options_frame, topOI)
    return options_frame, OI_df

if __name__ == '__main__':
    load_dotenv("../Prod_config/Stk_eodfetch.env") #Check path for env variables
    logging.basicConfig(filename=f'logging/optchain_{datetime.today().date()}.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    tzNow = DU.nowbyTZ('US/Eastern')
    section='PM'
    # run this after 4:30pm EST for market closing prices
    if (tzNow.hour < 9):
        tzNow = tzNow - timedelta(days=1)
    elif (tzNow.hour < 13):
        section='AM'

    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--Sect', dest='InSect', type=str)
    parser.add_argument('-D', '--Date', dest='InDate', type=str)
    parser.add_argument('-U', '--Upload', dest='upload', action='store_true', default=False)
    parser.add_argument('-c', '--check', dest='checkFlag', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='forceFlag', action='store_true', default=False)

    args = parser.parse_args()

    logging.debug(f'argments: {args}')
    if args.InSect is not None:
        section = args.InSect
    if args.InDate is not None:
        todt = datetime.strptime(args.InDate, '%Y-%m-%d').date()
    else:
        todt = tzNow.date()

    logging.info(f'optchain_fetch on {todt}-{section}; upload is {args.upload}')

    llists = ['etf_list', 'stock_list','us-cn_stock_list']
    topOIn = 5
    DB=environ.get("DBMKTDATA")
    opt_tbl=environ.get("TBLOPTCHAIN")
    fullopttbl = f'{DB}.{opt_tbl}'

    if not (todt.isoweekday() in range(1,6) or args.forceFlag):
        print('Jobs must be ran from Monday to Friday. use -f otherwise')
        quit()

    if args.checkFlag:
        quit()

    count = 0
    for listn in llists:
        slist = DU.get_Symbollist(listn)
        all_chains = pd.DataFrame()
        all_topOI = pd.DataFrame()
        csvN = os.path.join(LOADDIR, f'{listn}_{todt}-{section}.csv')
        csvOIN = os.path.join(LOADDIR, f'{listn}_{todt}-{section}_top{topOIn}.csv')
        for sym in slist:
            # maxD, sect = DU.get_Max_Options_date(fullopttbl, sym)
            # logging.debug(f'{sym} current MaxDate: {maxD}-{sect}')
            a_chain, aOI_df = ProcessOptions(sym, todt, section, topOIn, True)
            if len(a_chain)>0:
                all_chains = pd.concat([all_chains, a_chain])
                all_topOI = pd.concat([all_topOI, aOI_df])
        all_chains.to_csv(csvN, index=False)
        if args.upload:
            logging.info(f'Storing {csvN} in database')
            DU.StoreEOD(all_chains, " ", opt_tbl)

