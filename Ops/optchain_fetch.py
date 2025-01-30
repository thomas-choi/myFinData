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

max_retries = 5
retry_delay = 5

def option_chains(ticker, underlyingPrice=None):
    """https://www.pythonanywhere.com/user/twmchoi2022/files/home/twmchoi2022/myFinData/Ops
    """
    for retry in range(max_retries):

        chains = pd.DataFrame()
        try:
            asset = yf.Ticker(ticker)
            histdata = asset.history(period='1d')
            underlyingPrice = histdata['Close'].iloc[-1]
                
            expirations = asset.options
            logging.debug(f'{ticker} option chain: {expirations}')

            for expiration in expirations:
                # tuple of two dataframes
                opt = asset.option_chain(expiration)

                calls = opt.calls
                calls['OptionType'] = "call"

                puts = opt.puts
                puts['OptionType'] = "put"

                chain = pd.concat([calls, puts])
                chain['Expiration'] = pd.to_datetime(expiration)

                chains = pd.concat([chains, chain])
            chains['UnderlyingSymbol'] = ticker
            chains['UnderlyingPrice'] = underlyingPrice

            return chains
        except Exception as e:
            logging.error(f"asset.option({ticker}) error: {e}")
            if retry < max_retries-1:
                logging.error(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

    return pd.DataFrame()

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

def  current_mkt_price(tickers, process_dt):
    start_dt =  process_dt - timedelta(days=1)
    end_dt = process_dt +  timedelta(days=1)
    data = yf.download(tickers, start=start_dt, end=end_dt, group_by='ticker')
    data = data.ffill()
    current_prices = {}
    for ticker in tickers:
        current_prices[ticker] = data[ticker]['Close'].iloc[-1]
    return current_prices

def  getStrikes(in_df):
    strike_l = list(in_df['strike'].unique())
    strike_l.sort()
    return strike_l

def filter_opt_chain(i_df):
    data = i_df[i_df['lastPrice'] > 0.05]
    putdata = data[data['OptionType'] == 'put']
    calldata = data[data['OptionType'] == 'call']
    OI75 = i_df['openInterest'].quantile(0.25)
    put75 = putdata[putdata['openInterest']>OI75]
    call75 = calldata[calldata['openInterest']>OI75]
    return put75, call75

def ProcessOptions(ticker, underlying_px, process_dt, section, topOI, usedFile=False):
    logging.debug(f'ProcessOptions {ticker} on {process_dt}:{section}')
    underlying_symbol = ticker
    csvN = os.path.join(DATADIR, f'{underlying_symbol}_{process_dt}-{section}.csv')
    if os.path.exists(csvN) and usedFile:
        # read the original frame in from cache (pickle)
        logging.info(f'Recover data from {csvN}')
        try:
            options_frame = pd.read_csv(csvN)
            logging.debug(options_frame.head(2))
        except Exception as e:
            logging.error(f"asset.option({ticker}) open {csvN} error: {e}")
            options_frame = pd.DataFrame()
    else:
        # define a Options object
        logging.info(f'Download {underlying_symbol} option chain')
        options_frame = option_chains(underlying_symbol, underlying_px)
        # let's pickle the dataframe so we don't have to hit the network every time
        if usedFile:
            options_frame.to_csv(csvN, index=False)
            logging.debug(f' Save file {csvN}')
    if len(options_frame) > 0:
        put_df, call_df = filter_opt_chain(options_frame)
        logging.debug("putdf")
        logging.debug(put_df.head(2))
        logging.debug("call_df")
        logging.debug(call_df.head(2))
        options_frame = pd.concat([put_df, call_df], axis=0)
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
    logging.getLogger().setLevel(logging.INFO)

    tzNow = DU.nowbyTZ('US/Eastern')
    section='PM'
    # run this after 4:30pm EST for market closing prices
    if (tzNow.hour < 9):
        tzNow = tzNow - timedelta(days=1)
    elif (tzNow.hour < 13):
        section='AM'

    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--Sect', dest='Section', type=str)
    parser.add_argument('-D', '--Date', dest='Date', type=str)
    parser.add_argument('-t', '--test', dest='test', action='store_true', default=False)
    parser.add_argument('-U', '--Upload', dest='upload', action='store_true', default=False)
    parser.add_argument('-c', '--check', dest='checkFlag', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='forceFlag', action='store_true', default=False)
    parser.add_argument('-m', '--master', dest='master_db_list', action='store_true', default=False)
    parser.add_argument('-b', '--batchsize', dest='batchsize', type=int, default=30)
    parser.add_argument('-d', '--delay', dest='delay', type=int, default=retry_delay)

    args = parser.parse_args()

    logging.debug(f'argments: {args}')
    if args.Section is not None:
        section = args.Section
    if args.Date is not None:
        todt = datetime.strptime(args.Date, '%Y-%m-%d').date()
    else:
        todt = tzNow.date()
    retry_delay = args.delay

    logging.info(f'optchain_fetch on {todt}-{section}; upload is {args.upload}')

    llists = ['etf_list', 'stock_list','us-cn_stock_list']
    if args.master_db_list:
        llists=["master_db_list"]
    elif args.test:
        llists=['test_list']

    topOIn = 5
    DB=environ.get("DBMKTDATA")
    opt_tbl=environ.get("TBLOPTCHAIN")
    fullopttbl = f'{DB}.{opt_tbl}'

    if not (todt.isoweekday() in range(1,6) or args.forceFlag):
        print('Jobs must be ran from Monday to Friday. use -f otherwise')
        quit()

    if args.checkFlag:
        quit()

    nColumns=['Date','Section','UnderlyingSymbol','strike','Expiration','OptionType','contractSymbol',
    'lastTradeDate','lastPrice','bid','ask','change','percentChange','volume',
    'openInterest','impliedVolatility','inTheMoney','contractSize','currency','UnderlyingPrice']

    count = 0
    for listn in llists:
        slist = DU.load_symbols(listn)
        all_chains = pd.DataFrame()
        all_topOI = pd.DataFrame()
        csvN = os.path.join(LOADDIR, f'{listn}_{todt}-{section}.csv')
        csvOIN = os.path.join(LOADDIR, f'{listn}_{todt}-{section}_top{topOIn}.csv')
        logging.info(f'Symbol list {listn} : {slist}')
        # cur_prices = current_mkt_price(slist, todt)
        for sym in slist:
            # maxD, sect = DU.get_Max_Options_date(fullopttbl, sym)
            # logging.debug(f'{sym} current MaxDate: {maxD}-{sect}')

            a_chain, aOI_df = ProcessOptions(sym, None, todt, section, topOIn, True)
            if len(a_chain)>0:
                all_chains = pd.concat([all_chains, a_chain])
                all_topOI = pd.concat([all_topOI, aOI_df])
                if args.upload:
                    logging.info(f'Storing {sym} optchain in database')
                    saveDF=a_chain[nColumns]
                    DU.StoreEOD(saveDF, DB, opt_tbl)    
        if not args.upload:
            all_chains.to_csv(csvN, index=False)
            all_topOI.to_csv(csvOIN, index=False)
        # if args.upload:
        #     logging.info(f'Storing {csvN} in database')
        #     saveDF=all_chains[nColumns]
        #     DU.StoreEOD(saveDF, DB, opt_tbl)

