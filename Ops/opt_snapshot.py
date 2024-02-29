from datetime import datetime, date
import logging
from dotenv import load_dotenv
import dataUtil as DU
import time
from os import environ
import pandas as pd
import sys
import pytz
import sched
from ibAPIClient import IBClient
from ibapi.wrapper import *
import threading

localrun = False
client = None
opt_quotes = dict()
opt_contracts_lst = []
gwip = '172.27.96.1'
gwport = 7496

def run(event, context):
    logging.info(f"** ==> opt_snapshot.run(event: {event}, context: {context}")
    ny_datetime = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    today_str = ny_datetime.strftime('%Y-%m-%d')
    logging.info(f"Current opt_snapshot: NY Time: {ny_datetime}, today_string: {today_str}")

    opt_cols = ["Symbol","PnC","Strike","Expiration"]
    opt_df = pd.DataFrame(columns=opt_cols)
    logging.debug(opt_df.head())

    df = DU.load_df_SQL(f'call Trading.sp_stock_trades_V3;')
    df['Date'] = df['Date'].astype(str)
    df['Expiration'] = df['Expiration'].astype(str)
    df = df[df['Expiration'] >= today_str]
    print(df.head(2))
    for ix, row in df.iterrows():
        opt_df.loc[len(opt_df)] = [row.Symbol, row.PnC, row.Strike, row.Expiration]

    df = DU.load_df_SQL(f'call Trading.sp_etf_trades_v2;')
    df['Date'] = df['Date'].astype(str)
    df['Expiration'] = df['Expiration'].astype(str)
    df = df[df['Expiration'] >= today_str]
    print(df.head(2))
    for ix, row in df.iterrows():
        opt_df.loc[len(opt_df)] = [row.Symbol, row.PnC, row.H_Strike, row.Expiration]
        
    def keyformat(sym, pnc, strike, expire):
        return f'{sym}-{pnc}-{strike:.2f}-{expire}'
    
    # logging.DEBUG(opt_df)
    opt_df['KEY'] = opt_df.apply(lambda row: keyformat(row['Symbol'],row['PnC'],
                                               row['Strike'],row['Expiration']), axis=1)
    opt_df.sort_values(by=['Symbol','PnC','Strike','Expiration'], inplace=True)
    dup_values = opt_df['KEY'].duplicated()
    opt_df = opt_df[~dup_values]
    opt_df = opt_df.reset_index(drop=True)
    if localrun:
        opt_df.to_csv("options_list.csv", index=False)
    logging.info(opt_df)
    logging.info(f"Total size : {opt_df.shape}")

    for ix, row in opt_df.iterrows():
        logging.info(f"row:{ix} contents: {row}")
        contract = Contract()
        contract.symbol = row.Symbol
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.secType = "OPT"
        contract.lastTradeDateOrContractMonth = datetime.strptime(row.Expiration, "%Y-%m-%d").strftime('%Y%m%d')
        contract.strike = row.Strike
        contract.right = row.PnC
        if ix not in opt_quotes:
            opt_quotes[ix] = IBClient.create_opt_quote()
        # setup the fields of option quotes
        opt_quotes[ix]['Symbol'] = row.Symbol
        opt_quotes[ix]['PnC'] = row.PnC
        opt_quotes[ix]['Strike'] = row.Strike
        opt_quotes[ix]['Expiration'] = row.Expiration

        opt_contracts_lst.append(contract)

    logging.info(f" Created contract list: {opt_contracts_lst}")
    logging.info(f" Created quotes: {opt_quotes}")
    client = IBClient(opt_contracts_lst, opt_quotes)
    logging.info(f"App.connect({gwip},{gwport})")
    client.connect(gwip, gwport, 1)
    return client

next_request = 180
Init_image = True

def request_snapshot(scheduler, client):
    global next_request, Init_image

    logging.info(f"** ==> request_snapshot()")
    ny_datetime = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    data_timestamp = ny_datetime.strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Current opt_snapshot: NY Time: {ny_datetime}")

    # ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
    # logging.info(f"Current NY Time: {ny_time}")

    # Request the market data for the option contract
    DBMKTDATA=environ.get("DBMKTDATA")
    TBLSNAPSHOOT="options_snapshot"
    if client.c_quotes:
        snapshots = pd.DataFrame.from_dict(client.c_quotes, orient='index')
        snapshots['timestamp'] = data_timestamp
        snapshots.rename(columns={'LAST':'lastPrice','VOLUME':'volume','HIGH':'high','LOW':'low','CLOSE':'PClose','BID':'bid','ASK':'ask'}, inplace=True)
        logging.info('snapshot:)')
        logging.info(snapshots.info())
        logging.info("\n========\n")
        logging.info(snapshots)
        if Init_image:
            Init_image = False
        else:
            if localrun:
                snapshots.to_csv(f"{TBLSNAPSHOOT}.csv", index=False)
            else:
                snapshots.to_csv(f"{TBLSNAPSHOOT}.csv", index=False)
                DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
                DU.StoreEOD(snapshots, DBMKTDATA, TBLSNAPSHOOT)

    for i in range(len(client.c_list)):
        contract = client.c_list[i]
        time.sleep(1)
        logging.info(f"req: {contract}")
        client.reqMktData(i, contract, "", True, False, [])
    # Schedule the next call after 10 minutes
    scheduler.enter(next_request, 1, request_snapshot, (scheduler, client))
    if next_request < 200:
        next_request = 360
    elif next_request < 400:
        next_request = 480
    elif next_request < 600:
        next_request = 600

    # options_columns=opt_cols + ["contractSymbol", "lastTradeDate","strike","lastPrice","bid","ask","change","percentChange",
    #                  "volume","openInterest","impliedVolatility","inTheMoney","contractSize","currency", "PClose", "timestamp"]
    # snapshots = pd.DataFrame(columns=options_columns)
    # snapshots = snapshots[opt_cols + ["lastPrice","bid","ask","change","percentChange","volume","openInterest","impliedVolatility",
    #                                   "inTheMoney","contractSize","currency","PClose",'timestamp']]
    # snapshots.loc[len(snapshots)] = [row.Symbol, row.PnC, row.Strike, row.Expiration] + opt_values + [pclose, ny_time]

def request_subscript(client):
    for i in range(len(client.c_list)):
        contract = client.c_list[i]
        time.sleep(2)
        logging.info(f"req: {contract}")
        client.reqMktData(i, contract, "", False, False, [])

if __name__ == '__main__':
    logging.basicConfig(filename=f'opt_snapshot{datetime.today().date()}.log', filemode='a', 
                        format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', 
                        level=logging.INFO)
    load_dotenv("../Prod_config/Stk_eodfetch.env") #Check path for env variables
    localrun = False
    ibclient = run(0, 0)

    # set market data type 2 for frozen data, 1 for real-time data during market open
    ibclient.reqMarketDataType(2)

    client_thread = threading.Thread(target=ibclient.run)
    client_thread.start()

    # Create a scheduler object
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(0, 1, request_snapshot, (scheduler, ibclient))

    scheduler.run()

