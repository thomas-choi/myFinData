import os
from os import path
from os import environ
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging
from datetime import datetime, timedelta
import pytz
import time
import sshtunnel
from dotenv import load_dotenv, dotenv_values

class SQLDB:

    def __init__(self, envfile):
        self.dbconn = None
        self.sTunnel = None
        logging.info(f'SQLDB using env: {envfile}')
        self.config = pd.Series(dotenv_values(envfile))
        logging.debug(f'SQLDB using config: {self.config}')

    def setDBSSH(self):

        # Once has the SSH set, the following DB calls go thought SSH
        if self.sTunnel is not None:
            self.sTunnel.close()
        SSHHOST=self.config["SSHHOST"]
        SSHUSR=self.config["SSHUSR"]
        SSHPWD=self.config["SSHPWD"]
        DBHOST=self.config["DBHOST"]
        DBUSER=self.config["DBUSER"]
        DBPWD=self.config["DBPWD"]
        DB = self.config["DBMKTDATA"]
        DBPORT = int(self.config["DBPORT"])
        logging.debug(f'SSHHOST is {SSHHOST}, SSHUSR is {SSHUSR}, SSHPWD is {SSHPWD}')
        self.sTunnel = sshtunnel.SSHTunnelForwarder(
            (SSHHOST),ssh_username=SSHUSR, ssh_password=SSHPWD,
            remote_bind_address=(DBHOST, DBPORT)
            )
        logging.debug(f'sTunnel: {self.sTunnel}')
        self.sTunnel.start()
        dbpath = "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(host="127.0.0.1",
            db=DB, user=DBUSER, pw=DBPWD, port=self.sTunnel.local_bind_port)
        logging.info(f'setup DBengine to {dbpath}')
        # Create SQLAlchemy engine to connect to MySQL Database
        self.dbconn = create_engine(dbpath)
        logging.debug(f'dbconn=>{self.dbconn}')

    def get_DBengine(self):
        if self.dbconn is None:
            hostname=self.config["DBHOST"]
            uname=self.config["DBUSER"]
            pwd=self.config["DBPWD"]
            DB = self.config["DBMKTDATA"]
            DBPORT = self.config["DBPORT"]

            dbpath = "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(host=hostname, db=DB, user=uname, pw=pwd,port=DBPORT)
            logging.info(f'setup DBengine to {dbpath}')
            # Create SQLAlchemy engine to connect to MySQL Database
            self.dbconn = create_engine(dbpath)
            logging.info(f'dbconn=>{self.dbconn}')
        return self.dbconn


    def get_Max_date(self, dbntable, symbol=None):
        try:
            if symbol is None:
                query = f"SELECT max(Date) as maxdate from {dbntable} ;"
            else:
                query = f"SELECT max(Date) as maxdate from {dbntable} where symbol = \'{symbol}\';"
            logging.info(f'get_Max_date :{query}')
            df = pd.read_sql(query, self.get_DBengine())
            max_date = df.maxdate.iloc[0]
            logging.info(f"get_Max_date() => {max_date} ")
            return max_date
        except Exception as e:
            logging.error("Exception occurred at get_Max_date()", exc_info=True)

    def get_Max_Options_date(self, dbntable, symbol=None):
        try:
            if symbol is None:
                query = f'SELECT max(Date) as maxdate, section FROM {dbntable} group by section order by maxdate desc, section desc;'
            else:
                query = f'SELECT max(Date) as maxdate, section FROM dbobj,{dbntable} where UnderlyingSymbol = \'{symbol}\' group by section order by maxdate desc, section desc;'

            logging.info(f'get_Max_date :{query}')
            df = pd.read_sql(query, self.get_DBengine())
            logging.debug(df.head())
            max_date = df.maxdate.iloc[0]
            section = df.section.iloc[0]
            logging.info(f"get_Max_date() => {max_date},{section} ")
            return max_date, section
        except Exception as e:
            logging.error("Exception occurred at get_Max_Options_date()", exc_info=True)

    def get_Latest_row_by_Symbol(self, dbntable, symbol):
        try:
            query = f"SELECT * from {dbntable} where Symbol = \'{symbol}\' order by Date desc;"
            logging.info(f'get_Latest_row_by_Symbol :{query}')
            df = pd.read_sql(query, self.get_DBengine())
            if len(df) > 0:
                max_row = df.iloc[0]
            else:
                max_row = None
            return max_row
        except Exception as e:
            logging.error("Exception occurred at get_Latest_row_by_Symbol()", exc_info=True)

    def ExecSQL(self, query):
        logging.info(f"ExecSQL: {query}")
        try:
            results = self.get_DBengine().execute(query)
            logging.info(f'number of rows execed: {results.rowcount}')
        except Exception as e:
            logging.error("Exception occurred at load_df(np.linspace)", exc_info=True)

    def load_df_SQL(self, query):
        """
        Return dataframe from SQL statement
        """
        logging.info(f'load_df_SQL({query}).')
        try:
            df = pd.read_sql(query, self.get_DBengine())
            return df
        except Exception as e:
            logging.error("Exception occurred at load_df_SQL(np.linspace)", exc_info=True)

    def load_df(self, stock_symbol=None, DailyMode=True, lastdt=None, startdt=None, dataMode="P"):
        """
        Return dataframe from histdailyprice3
        """
        HOST=self.config["DBHOST"]
        PORT=self.config["DBPORT"]
        USER=self.config["DBUSER"]
        PASSWORD=self.config["DBPWD"]
        logging.info(f'load_df ( {stock_symbol},{DailyMode},{lastdt},{startdt},{dataMode}).')    
        dpath = None
        if dataMode == "P":
            DBNAME=self.config["DBMKTDATA"]
            TBLName=self.config["TBLDLYPRICE"]
            if stock_symbol is not None:
                dpath = f"{TBLName}/{stock_symbol}.csv"
            elif (lastdt is not None) and (startdt is not None):
                dpath = f"{TBLName}/{startdt}-{lastdt}.csv"
        else:
            DBNAME=self.config["DBPREDICT"]
            TBLName=self.config["TBLDAILYOUTPUT"]

        if (dpath is not None) and path.isfile(dpath) and (not DailyMode):
            logging.info(f'Load data from {dpath}.')
            return pd.load_csv(dpath)
        else:
            logging.info(f'Load data from {DBNAME}.{TBLName} table in MySQL, Daily mode: {DailyMode}.')
            try: 
                wherecl=""
                if stock_symbol is not None:
                    wherecl = f"where Symbol = '{stock_symbol}'"
                if startdt is not None:
                    if len(wherecl) > 0:
                        wherecl = wherecl + f" and Date>='{startdt}'"
                    else:
                        wherecl = f"where Date>='{startdt}'"
                if lastdt is not None:
                    if len(wherecl) > 0:
                        wherecl = wherecl + f" and Date<='{lastdt}'"
                    else:
                        wherecl = f"where Date<='{lastdt}'"
                nlimit = ""
                # if (stock_symbol is not None) and (startdt is None) and (lastdt is None):
                #     if DailyMode:
                #         nlimit = f" order by Date desc limit {DailySize}"
                #     else:
                #         nlimit = f" order by Date desc limit {TrainSize}"
                if dataMode == "P":
                    query = f"SELECT Date, Symbol, Exchange, AdjClose, Close, Open, High, Low, Volume from {DBNAME}.{TBLName} {wherecl} {nlimit};"
                else:
                    query = f"SELECT * from {DBNAME}.{TBLName} {wherecl} {nlimit};"

                logging.info(f'load_df query:{query}')
                histdailyprice3 = pd.read_sql(query, self.get_DBengine())
                # conn.close()
                df = histdailyprice3.copy()
                df = df.sort_values(by=['Date'])
                if (dpath is not None):
                    df.to_csv(dpath, index=False)

                return df
            except Exception as e:
                logging.error("Exception occurred at load_df(np.linspace)", exc_info=True)

    def StoreEOD(self, eoddata, DBn, TBLn):
        try:
            eoddata.head(3)
            logging.info(f'StoreEOD size: {len(eoddata)} in table:{TBLn} on DB:{DBn}')
            dbcon = self.get_DBengine()
            logging.info(f'StoreEOD dbcon: {dbcon}')
            # Convert dataframe to sql table
            eoddata.to_sql(name=TBLn, con=dbcon, schema=DBn, if_exists='append', index=False)
        except Exception as e:
            logging.error("Exception occurred", exc_info=True)

    def StoreWebDaily(self, df):
        try:
            logging.info(f'StoreWebDaily size: {len(df)}')
            dbname=self.config["DBWEB"]
            table=self.config["TBLWEBPREDICT"]

            self.ExecSQL(f'TRUNCATE TABLE {dbname}.{table};')
            self.StoreEOD(df, dbname, table)
        except Exception as e:
            logging.error("Exception occurred at StoreDailyOutput()", exc_info=True)

    def load_eod_price(self, ticker, start, end):
        DB = self.config["DBMKTDATA"]
        TBL = self.config["TBLDLYPRICE"]
        query = f"SELECT * from {DB}.{TBL} where symbol = \'{ticker}\' and Date >= \'{start}\' and Date <= \'{end}\' order by Date;"
        return self.load_df_SQL(query)

    def get_Last_Date_by_Sym(self, tblname, sym):
        FIRSTTRAINDTE = datetime.strptime(self.config["FIRSTTRAINDTE"], "%Y/%m/%d").date()

        mktdate = self.get_Max_date(tblname, sym)
        lastdt = FIRSTTRAINDTE
        if mktdate is not None:
            lastdt = mktdate + timedelta(days=1)
        return lastdt
