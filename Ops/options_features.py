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

# df - rows of the options chain data
def   genOptionsFeaturesV2(df):
    dtlist = df['Date'].unique()

    flatdf = df.groupby(['Date','OptionType'])[['volume']].sum()
    flatdf['Symbol'] = df.groupby(['Date','OptionType'])[['UnderlyingSymbol']].first()
    flatdf['last'] = df.groupby(['Date','OptionType'])[['UnderlyingPrice']].first()
    print(flatdf)

    flatdf['MaxOI'] = 0.0
    flatdf['MaxOIStrike'] = 0.0
    flatdf['MaxOIExpire'] = 0.0
    flatdf['MaxOIImpVol'] = 0.0
    flatdf['PutCallratio'] = 0.0

    for tgdate in dtlist:
        try:
            pcratio = flatdf.loc[tgdate,'put']['volume']/flatdf.loc[tgdate,'call']['volume']
            print(f'P/C={pcratio} on {tgdate}')
            flatdf.at[(tgdate,'call'), 'PutCallratio'] = pcratio
            flatdf.at[(tgdate,'put'), 'PutCallratio'] = pcratio
        except Exception as error:
            # handle the exception
            print(f"An exception ({tgdate}) occurred:", error) # An exception occurred: division by zero
            flatdf.drop([tgdate], axis='index', inplace=True)

    for i,row in flatdf.iterrows():
        print(' first 2 index: ', i[0], i[1])
        selected = df[(df['OptionType']==i[1]) & (df['Date']==i[0])]
        maxstrike = selected.groupby(['strike'])[['openInterest']].sum().reset_index().sort_values(by=['openInterest'], ascending=False)
#         print(maxstrike)
        mstrike = maxstrike.iloc[0]['strike']
        mOI = maxstrike.iloc[0]['openInterest']
        print(f'max strike:{mstrike}, OI{mOI}')
        maxStrikeSelected = selected[selected['strike'] == mstrike].sort_values(by=['openInterest'], ascending=False)
#         print(maxStrikeSelected)
        max_row = maxStrikeSelected.iloc[0]
        print(f' the max_row: {max_row}')
        flatdf.at[i, 'MaxOI'] = mOI
        flatdf.at[i, 'MaxOIStrike'] = max_row['strike']
        flatdf.at[i, 'MaxOIExpire'] = max_row['Expiration']
        flatdf.at[i, 'MaxOIImpVol'] = max_row['impliedVolatility']
    return flatdf

def   genOptionsFeatures(df):
    dtlist = df['Date'].unique()
    logging.debug(f'Date List Length: {len(dtlist)}')

    flatdf = df.groupby(['Date','OptionType'])[['volume']].sum()
    flatdf['Symbol'] = df.groupby(['Date','OptionType'])[['UnderlyingSymbol']].first()
    flatdf['last'] = df.groupby(['Date','OptionType'])[['UnderlyingPrice']].first()
    print(flatdf)

    flatdf['MaxOI'] = 0.0
    flatdf['MaxOIStrike'] = 0.0
    flatdf['MaxOIExpire'] = 0.0
    flatdf['MaxOIImpVol'] = 0.0
    flatdf['PutCallratio'] = 0.0

    for tgdate in dtlist:
        try:
            pcratio = flatdf.loc[tgdate,'put']['volume']/flatdf.loc[tgdate,'call']['volume']
            flatdf.at[(tgdate,'call'), 'PutCallratio'] = pcratio
            flatdf.at[(tgdate,'put'), 'PutCallratio'] = pcratio
        except Exception as error:
            # handle the exception
            logging.error(f'An exception ({tgdate}) occurred: {error}') # An exception occurred: division by zero
            flatdf.drop([tgdate], axis='index', inplace=True)

    for i,row in flatdf.iterrows():
        print(i[0], i[1])
        max_row = df[(df['OptionType']==i[1]) & (df['Date']==i[0])].sort_values(by=['openInterest'],ascending=False).iloc[0]
        flatdf.at[i, 'MaxOI'] = max_row['openInterest']
        flatdf.at[i, 'MaxOIStrike'] = max_row['strike']
        flatdf.at[i, 'MaxOIExpire'] = max_row['Expiration']
        flatdf.at[i, 'MaxOIImpVol'] = max_row['impliedVolatility']
    return flatdf

def ProcessOptionsFeatures(ticker, enddt):
    logging.debug(f'ProcessOptionsFeatures({ticker}, {enddt})')
    # eod_df = DU.load_eod_price(ticker, startdt, enddt)
    sql=f"call GlobalMarketData.get_option_features(\'{ticker}\', \'{enddt}\');"
    df = DU.load_df_SQL(sql)
    logging.debug(f'option chain size: {df.shape}')

    if len(df) > 0:
        return genOptionsFeaturesV2(df)
    else:
        return df

if __name__ == '__main__':
    load_dotenv("../Prod_config/Stk_eodfetch.env") #Check path for env variables
    logging.basicConfig(filename=f'logging/opt_features_{datetime.today().date()}.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    tzNow = DU.nowbyTZ('US/Eastern')

    # run this after 4:30pm EST for market closing prices
    if (tzNow.hour < 9):
        tzNow = tzNow - timedelta(days=1)

    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--Date', dest='Date', type=str)
    parser.add_argument('-t', '--test', dest='test', action='store_true', default=False)
    parser.add_argument('-U', '--Upload', dest='upload', action='store_true', default=False)
    parser.add_argument('-c', '--check', dest='checkFlag', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='forceFlag', action='store_true', default=False)
    # test process parameters
    # -D [] -t -c 
    args = parser.parse_args()

    logging.debug(f'argments: {args}')
    if args.Date is not None:
        todt = datetime.strptime(args.Date, '%Y-%m-%d').date()
    else:
        todt = tzNow.date()

    logging.info(f'option featres on {todt}; upload is {args.upload}')

    llists = ['etf_list', 'stock_list','us-cn_stock_list']
    if args.test:
        llists=['test_list']
    logging.info(f'symbol list: {llists}')
    topOIn = 5
    DB=environ.get("DBMKTDATA")
    opt_tbl=environ.get("TBLOPTFEATURE")

    if not (todt.isoweekday() in range(1,6) or args.forceFlag):
        print('Jobs must be ran from Monday to Friday. use -f otherwise')
        quit()

    if args.checkFlag:
        quit()

    count = 0
    csvN = os.path.join('opt_features', f'{todt}-ALL.csv')
    all_features = pd.DataFrame()
    storelist=['Date','Symbol','OptionType','volume','last','MaxOI','MaxOIStrike','MaxOIExpire','MaxOIImpVol','PutCallratio']

    for listn in llists:
        slist = DU.get_Symbollist(listn)
        for sym in slist:
            # maxD, sect = DU.get_Max_Options_date(fullopttbl, sym)
            # logging.debug(f'{sym} current MaxDate: {maxD}-{sect}')
            opt_features = ProcessOptionsFeatures(sym, todt)
            if len(opt_features) < 1:
                continue
            draw= opt_features.reset_index()[storelist]
            
            if len(draw) < 10:
                all_features = pd.concat([all_features, draw])
            else:
                print(draw)
                symN = os.path.join('opt_features', f'{todt}-{sym}.csv')
                draw.to_csv(symN, index=False)
                if args.upload:
                    logging.info(f'Storing {symN} in database')
                    DU.StoreEOD(draw, DB, opt_tbl)
    all_features.to_csv(csvN, index=False)
    if args.upload:
        logging.info(f'Storing {csvN} in database')
        DU.StoreEOD(all_features, DB, opt_tbl)

