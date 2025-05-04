import os
from os import path
from os import environ
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging
from dateutil.rrule import rrule, DAILY
import yfinance as yf
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import time
import argparse
import dataUtil as DU
from tiingo import TiingoClient

load_dotenv()

dbconn = None
tii_client = None
tii_i=0

tiikey1 = environ.get("TIIKEY1")
tiikey2 = environ.get("TIIKEY2")

tii_config = [{
                'api_key': f'{tiikey1}',
                'session': True  # Reuse the HTTP session for better performance
            }, 
            {
                'api_key': f'{tiikey2}',
                'session': True  # Reuse the HTTP session for better performance
            }]

def fetch_eoddata(quotes, exch, startD, endD):
    try:

        rlist = []
        for i in range(len(quotes)):
            try:
                q = quotes[i]
                rec = {"Date":q.quote_datetime, "Symbol":q.symbol, "Exchange":exch, "Close":q.close, "Open":q.open, "High":q.high, "Low":q.low,'Volume':q.volume}
                logging.info(f'fetch eod: {rec}')
                if q.quote_datetime >= startD and q.quote_datetime <= endD:
                    rlist.append(rec)
            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
        return rlist
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

def yf_download(sym,sdate,edate):
    sDF = yf.download(sym,sdate,edate, auto_adjust=False)
    if len(sDF) > 0:
        sDF = sDF.reset_index()
        # from 2025/2/25, 'Adj Close' is not existed from API
        if 'Adj Close' in sDF.columns:
            sDF = sDF.rename(columns={'Adj Close':'AdjClose'})
    return sDF

def ti_client():
    global tii_client, tii_i

    if tii_client is None:
        logging.debug(f"tii_config({tii_i}):= ", tii_config[tii_i])
        tii_client = TiingoClient(tii_config[tii_i])
        tii_i = (tii_i + 1) % len(tii_config)
    return tii_client


def tii_download(sym,sdate,edate):
    global tii_client
    try:
        sDF = ti_client().get_dataframe(sym, startDate=sdate, endDate=edate, frequency='daily')
        if len(sDF)>0:
            sDF = sDF.reset_index()
            sDF = sDF.rename(columns={'volume':'Volume','date':'Date','close':'Close','high':'High','low':'Low','open':'Open','adjClose':'AdjClose'})
        return sDF
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        tii_client=None
        return pd.DataFrame()

def check_exchange_from_ticker(ticker):
    st = ticker.split(".")
    if len(st)>1:
        return st[-1]
    else:
        return ""
    
def common_fetch_eod(sdate, tdate, list_name, dbFlag=True): 
    vendor = environ.get("VENDOR")
    logging.info(f'common_fetch_eod handle {sdate} UPTO {tdate}, list_name={list_name}, dbFlag={dbFlag}, vendor={vendor}')

    symbol_list = DU.load_symbols(list_name)
    if len(symbol_list) <= 0:
        return
    exch_dict = DU.load_symbols_dict()
    DBMKTDATA = environ.get("DBMKTDATA")
    TBLDAILYPRICE = environ.get("TBLDLYPRICE")
    # result=pd.DataFrame()
    savColumns = ['Date','Symbol','Exchange','Close','Open','High','Low','Volume','AdjClose']
    datallist = list()
    for sym in symbol_list:
        sdate = DU.get_Last_Date_by_Sym(f'{DBMKTDATA}.{TBLDAILYPRICE}', sym)
        if tdate >= sdate:
            dlypath = f'{vendor}/{sym}_{sdate}_{tdate}.csv'
            if path.isfile(dlypath):
                logging.info(f'Loading {sym} daily OHLC from {dlypath}')
                sDF = pd.read_csv(dlypath)
            else:
                edate = tdate+ timedelta(days=1)
                logging.info(f'Loading {sym} daily OHLC from Yahoo {sdate} to {edate}! ')
                if vendor == "yfinance":
                    sDF = yf_download(sym,sdate,edate)
                elif vendor=="tiingo":
                    sDF = tii_download(sym,sdate,edate)

                # sDF.columns = [col[0] for col in sDF.columns]
                logging.debug(f"{sym} is downloaded DF : {sDF}")
                if len(sDF) > 0:
                    # remove multiIndex
                    if sDF.columns.nlevels>1:
                        sDF.columns =  [ col[0] for col in sDF.columns]
                    sDF['Symbol']=sym
                    if sym in exch_dict:
                        sDF['Exchange'] = exch_dict[sym]
                    elif (sym == "^HSI"):
                        sDF['Exchange'] = "HK"
                    else:
                        sDF['Exchange'] = check_exchange_from_ticker(sym)
                    sDF = sDF[savColumns]
                    sDF['Date'] = pd.to_datetime(sDF['Date']).dt.date
                    logging.info(f'common_fetch_eod: downloaded {sym} from {sDF.Date.iloc[0]} to {sDF.Date.iloc[-1]}')
                    sDF = sDF[(sDF['Date'] >= sdate) & (sDF['Date'] <= tdate)]
                    sDF.to_csv(dlypath, index=False)
            if len(sDF) > 0:
                if len(sDF)>100:
                    logging.debug(sDF)
                    DU.StoreEOD(sDF, None, TBLDAILYPRICE)
                else:
                    datallist.append(sDF)
    if dbFlag and len(datallist)>0:
        totalDF = pd.concat(datallist)
        dlypath = f'{vendor}/{list_name}_{sdate}_{tdate}.csv'
        logging.info(f'outputing {dlypath}')
        totalDF.to_csv(dlypath, index=False)
        # if debug turn-off below
        totalDF.Date = pd.to_datetime(totalDF.Date)
        DU.StoreEOD(totalDF, None, TBLDAILYPRICE)
        # totalDF.to_sql(name=TBLDAILYPRICE, con=get_DBengine(), if_exists='append', index=False)
    logging.info(f'common_fetch_eod finish the handle {list_name} UPTO {tdate}')

def fetch_by_date(sDate, eDate, listname, dbf=True):
    vendor = environ.get("VENDOR")
    if vendor=="tiingo":
        tiingo_fetch_by_date(sDate, eDate, listname, dbf)
    elif vendor=="eoddata":
        exchanges = ['NYSE','AMEX','NASDAQ']
        for dt in rrule(DAILY, dtstart=Sdate, until=mToday):
            if dt.weekday() not in range(0, 5):
                logging.info(f'{dt} is not on weekday')
            else:
                fetch_by_exchanges(dt, exchanges)
    elif vendor == 'yfinance':
        yfinance_fetch_eod(sDate, eDate, listname, dbf)


def fetch_by_symbols(Sdate, Edate):
    # read csv file
    try:
        logging.info(f'fetch_by_symbols from {Sdate} to {Edate}')
        symlist = pd.read_csv("stock_list.csv", names=['Symbol','Exchange'])
        client = EodDataHttpClient(username='thomaschoi', password='905916Tc')

        datap = f'eoddata_{Sdate.date()}-{Edate.date()}.csv'

        if path.isfile(datap):
            logging.info(f'Loading EODDATA from {datap}')
            eoddf = pd.read_csv(datap)
        else:
            rows = []
            for i, r in symlist.iterrows():
                try:
                    logging.info(f'fetch EODDATA {r.Symbol},{r.Exchange} from {Sdate} to{Edate}')
                    quotes = client.symbol_history_period_by_range(exchange_code=r.Exchange, symbol=r.Symbol,
                        start_date=Sdate, end_date=Edate, period='D')
                    # quotes = client.quote_detail(exchange_code=exch, symbol=sym)

                    r = fetch_eoddata(quotes, r.Exchange, Sdate, Edate)
                    if (r is not None):
                        logging.debug(f"Type: {type(r)}, LEN:{len(r)}")
                        rows += r
                except Exception as e:
                    logging.error("Exception occurred", exc_info=True)

            eoddf = pd.DataFrame(rows)
            eoddf = eoddf.sort_values(by='Date')
            eoddf.to_csv(datap,index = False)

        eoddf['Date'] = pd.to_datetime(eoddf['Date'])
        # StoreEOD(eoddf)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

def daily_output_columns():
    return ["Date", "Symbol", "Exchange", "garch", "svr", "mlp", "LSTM", "prev_Close", "prediction", "volatility"]
    # return ["Date", "Symbol", "Exchange", "garch", "svr","svrRP", "mlp","mlpRP", "LSTM", "prev_Close", "prediction", "volatility"]

def init_daily_output(Edate):
    logging.info(f"eoddata.init_daily_output {Edate}")
    DBHOST = environ.get("DBHOST")
    DBPORT = environ.get("DBPORT")
    DBUSER = environ.get("DBUSER")
    DBPWD = environ.get("DBPWD")
    DBMKTDATA = environ.get("DBMKTDATA")
    DBPREDICT = environ.get("DBPREDICT")
    TBLDAILYOUTPUT = environ.get("TBLDAILYOUTPUT")
    TBLDLYPRICE = environ.get("TBLDLYPRICE")
    TBLDAILYPERF = environ.get("TBLDAILYPERF")

    try:
        Sdate = Edate-datetime.timedelta(days=300)
        logging.info(f" Reset from {Sdate} to {Edate}")
        symbol_list = load_symbols()
        rowlist = pd.DataFrame()
        for symbol in symbol_list.Symbol:
            query = f"SELECT Date,Symbol, Exchange, Close as prev_Close from {DBMKTDATA}.{TBLDLYPRICE} WHERE Symbol='{symbol}' and Date>='{Sdate}' and Date<='{Edate}';"
            logging.info(f'init_daily_output query:{query}')
            conn = mysql.connector.connect(host=DBHOST,port=DBPORT,user=DBUSER,password=DBPWD)
            df = pd.read_sql(query, conn)
            # add ActualPercent here
            # df['ActualPercent'] = (df.prev_Close - df.prev_Close.shift(-1)) * 100 / df.prev_Close
            rowlist = rowlist.append(df, ignore_index=True)
        logging.debug(rowlist)
        rowlist.to_sql(name=TBLDAILYPRICE, con=get_DBengine(), if_exists='append', index=False)

        # data.StoreEOD(rowlist, DBPREDICT, TBLDAILYOUTPUT)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

def get_daily_performance(Sdate, Edate):
    logging.info(f"EODDATA daily performance from {Sdate} to {Edate}")
    DBHOST = environ.get("DBHOST")
    DBPORT = environ.get("DBPORT")
    DBUSER = environ.get("DBUSER")
    DBPWD = environ.get("DBPWD")
    DBMKTDATA = environ.get("DBMKTDATA")
    DBPREDICT = environ.get("DBPREDICT")
    TBLDAILYOUTPUT = environ.get("TBLDAILYOUTPUT")
    TBLDLYPRICE = environ.get("TBLDLYPRICE")
    TBLDAILYPERF = environ.get("TBLDAILYPERF")

    try:
        mythreshold = 2 # Percent
        conn = mysql.connector.connect(host=DBHOST,port=DBPORT,user=DBUSER,password=DBPWD)
        Hdate = Sdate - datetime.timedelta(days=5)
        query = f"SELECT Date,Symbol, Exchange, garch,svr,mlp,LSTM,prev_Close, prediction, volatility from {DBPREDICT}.{TBLDAILYOUTPUT} WHERE Date>='{Hdate}' and Date<='{Edate}';"
        logging.info(f'load_df query:{query}')
        df = pd.read_sql(query, conn)
        symlist = df.Symbol.unique()
        logging.debug(f'symbol-list:{symlist}')
        outputdf = pd.DataFrame(columns=df.columns.values.tolist())
        logging.debug(f'{outputdf.columns.values.tolist()}')
        for symbol in symlist:
            closedf = df[df["Symbol"]==symbol].reset_index(drop=True)
            symdf = closedf[closedf['Date'] >= Sdate].reset_index(drop=True)
            symdf['ActualDate'] = symdf['Date'].shift(periods=-1, axis=0)
            symdf['ActualClose'] = symdf['prev_Close'].shift(periods=-1, axis=0)
            symdf = symdf[:-1]
            symdf['ActualPercent'] = abs((symdf['ActualClose'] - symdf['prev_Close'])/symdf['prev_Close']*100)
            closedf['returns'] = 100*closedf.prev_Close.pct_change()
            closedf['vol'] = closedf.returns.rolling(5).std()
            logging.debug(closedf[closedf["Date"]>=Sdate].vol)
            symdf["ActualStd"] = closedf[closedf["Date"]>=Sdate].vol
            symdf["close_change"] = symdf.loc[:, 'ActualClose'] - symdf.loc[:, 'prev_Close']
            symdf["price_movement"] = symdf["close_change"].apply(predict.get_price_movement)
            symdf["above_threshold"] = symdf["ActualPercent"].apply(lambda x: predict.get_above_threshold(x, mythreshold))
            symdf["ActualTrend"] = symdf.apply(predict.get_prediction, axis=1)
            symdf = symdf[symdf["Date"]>=Sdate]
            outputdf = outputdf.append(symdf, ignore_index=True)
            logging.debug(f"\n{symdf.head(1)}")
            logging.debug(f"\n{outputdf.tail(2)}")
        outputdf = outputdf.sort_values(by=['Date', 'Symbol','Exchange'])

        # logging.debug("**   Start close_change   **")
        # logging.debug(outputdf["close_change"])
        # logging.debug("**   Start price_movement   **")
        # logging.debug(outputdf["price_movement"])
        # logging.debug("**   Start above_threshold   **")
        # logging.debug(outputdf["above_threshold"])
        # logging.debug(outputdf["ActualTrend"])

        outputdf.drop(columns=['close_change', 'prDKNG_2022-11-01_2022-11-02ice_movement','above_threshold'], inplace=True)
        lastdt = outputdf.iloc[-1, outputdf.columns.get_loc('ActualDate')]
        outputdf.to_csv(f'daily_output/dailyPerf_{Sdate}_{lastdt}.csv',index = False)
        data.StoreEOD(outputdf, DBPREDICT, TBLDAILYPERF)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

def fetch_by_exchanges(Sdate, exchanges):
    try:
        logging.info(f'fetch_by_exchangall_listses {exchanges} on {Sdate}')
        MKDB=environ.get("DBMKTDATA")
        MKTBL=environ.get("TBLDLYPRICE")
        client = EodDataHttpClient(username='thomaschoi', password='905916Tc')
        datap = f'daily_output/eoddata_{Sdate.date()}.csv'

        if path.isfile(datap):
            logging.info(f'Loading EODDATA from {datap}')
            eoddf = pd.read_csv(datap)
        else:
            rows = []
            for exch in exchanges:
                try:
                    logging.info(f'fetch EODDAT {exch} on {Sdate}')
                    quotes = client.quote_list_by_date_period(exchange_code=exch,
                                                                date=Sdate, period='D')

                    r = fetch_eoddata(quotes, exch, Sdate, Sdate)
                    if (r is not None):
                        logging.debug(f"Type: {type(r)}, LEN:{len(r)}")
                        rows += r
                except Exception as e:
                    logging.error("Exception occurred", exc_info=True)
            logging.info(f'Downloaded total {len(rows)} of records')
            eoddf = pd.DataFrame(rows)
            eoddf = eoddf.drop_duplicates(subset=['Date', 'Symbol'], keep='last')
            eoddf = eoddf.sort_values(by='Date')
            eoddf.to_csv(datap,index = False)
        eoddf['Date'] = pd.to_datetime(eoddf['Date'])
        # eoddf.to_sql(name=MKTBL, con=get_DBengine(), if_exists='append', index=False)
        eoddf.StoreEOD(eoddf, MKDB, MKTBL)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

if __name__ == '__main__':
    logging.basicConfig(filename=f'logging/eoddata_ext_{datetime.today().date()}.log', filemode='a', format='%(asctime)s.%(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    tzNow = DU.nowbyTZ('US/Eastern')
    # run this after 4:30pm EST for market closing prices
    if (tzNow.hour < 9):
        tzNow = tzNow - timedelta(days=1)

    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--Date', dest='Date', type=str)
    parser.add_argument('-C', '--Config', dest='Config', type=str)
    parser.add_argument('-c', '--check', dest='checkFlag', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='forceFlag', action='store_true', default=False)
    parser.add_argument('-t', '--test', dest='test', action='store_true', default=False)
    parser.add_argument('-m', '--master', dest='master_db_list', action='store_true', default=False)

    args = parser.parse_args()

    logging.debug(f'argments: {args}')
    if args.Date is not None:
        todt = datetime.strptime(args.Date, '%Y-%m-%d').date()
    else:
        todt = tzNow.date()
    # if args.Config is not None:
    #     load_dotenv(args.Config)
    # else:
    #     # config_file = "../Prod_config/Stk_eodfetch.env"
    #     load_dotenv()
    # # load_dotenv(config_file) #Check path for env variables

    DBMKTDATA=environ.get("DBMKTDATA")
    DBPREDICT=environ.get("DBPREDICT")
    DBWEB=environ.get("DBWEB")
    TBLDAILYOUTPUT=environ.get("TBLDAILYOUTPUT")
    TBLDAILYPRICE=environ.get("TBLDLYPRICE")
    TBLDLYPRED=environ.get("TBLDLYPRED")
    TBLDLYLSTM=environ.get("TBLDLYLSTM")
    TBLWEBPREDICT=environ.get("TBLWEBPREDICT")
    FIRSTTRAINDTE = datetime.strptime(environ.get("FIRSTTRAINDTE"), "%Y/%m/%d").date()
    LASTTRAINDATE = datetime.strptime(environ.get("LASTTRAINDATE"), "%Y/%m/%d").date()

    mktdate = DU.get_Max_date(f'{DBMKTDATA}.{TBLDAILYPRICE}')
    if mktdate is None:
        Sdate = FIRSTTRAINDTE
    else:
        Sdate = mktdate + timedelta(days=1)

    tformats = '%Y-%m-%d %H:%M:%S'
    tzinfo = time.tzname
    tNow = datetime.now()
    logging.info(f'Local System TimeZone info: {tzinfo[0]} and local time: {tNow}')

    estNow = datetime.strptime(tNow.astimezone(pytz.timezone('US/Eastern')).strftime(tformats), tformats)
    logging.info(f'EST time: {estNow}')

    today5PM = estNow.replace(hour=17, minute=0, second=0, microsecond=0)
    logging.info(f'Current Date-Time is {estNow}, and cutt-off time is {today5PM}')

    mToday = estNow.date()
    if tNow < today5PM:
        mToday = mToday - timedelta(days=1)
    # debug override
    # mToday = datetime(2022,11,2).date()
    if not (todt.isoweekday() in range(1,6) or args.forceFlag):
        print('Jobs must be ran from Monday to Friday. use -f otherwise')
        quit()

    if args.checkFlag:
        quit()

    list_N = ["stock_list", "etf_list", "crypto_list", "us-cn_stock_list"]
    vendor = environ.get("VENDOR")
    if args.master_db_list:
        common_fetch_eod(Sdate, mToday, list_name="master_db_list")
    else:
        if args.test:
            list_N=['test_list']
        for symN in list_N:
            common_fetch_eod(Sdate, mToday, list_name=symN)
