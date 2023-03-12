import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import keras
import tensorflow as tf
from keras.models import Model
from keras.layers import Dense, Dropout, LSTM, Input, Activation
from keras import optimizers
from sklearn.metrics import mean_squared_error
from dotenv import load_dotenv
from os import path
from os import environ
import logging
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
from data import processing
import daily_gap_model as dgm
import dataUtil as DU

data_window = 21

if __name__ == '__main__':
    print('My file path is ', __file__)
    config_path = path.join("..", "Prod_config", "Stk_eodfetch_PythonAnywhere.env")
    if not path.isfile(config_path):
        print(f'Config File Path {config_path} is not existed')
        quit()
    load_dotenv(config_path) #Check path for env variables
    logging.basicConfig(filename=f'logging/apply_gmodel_{datetime.today().date()}.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    DBMKTDATA=environ.get("DBMKTDATA")
    DBPREDICT=environ.get("DBPREDICT")
    DBWEB=environ.get("DBWEB")
    TBLDAILYOUTPUT=environ.get("TBLDAILYOUTPUT")
    TBLDAILYPRICE=environ.get("TBLDLYPRICE")
    TBLDLYPRED=environ.get("TBLDLYPRED")
    TBLDLYLSTM=environ.get("TBLDLYLSTM")
    TBLWEBPREDICT=environ.get("TBLWEBPREDICT")
    print('1st date:', environ.get("FIRSTTRAINDTE"))
    FIRSTTRAINDTE = datetime.strptime(environ.get("FIRSTTRAINDTE"), "%Y/%m/%d").date()
    LASTTRAINDATE = datetime.strptime(environ.get("LASTTRAINDATE"), "%Y/%m/%d").date()

    def Process_Prediction(sym, mktdate):
        # get last previous prediction record from DB
        lastpred = DU.get_Latest_row_by_Symbol(f"{DBPREDICT}.{TBLDLYPRED}", sym)
        logging.debug(lastpred)
        lastPredClose = 0.0
        if lastpred is not None:
            lastPredClose = lastpred.predClose
        logging.debug(lastPredClose)

        if lastpred is None:
            Sdate = LASTTRAINDATE+ timedelta(days=1)    
        else:
            Sdate = lastpred.Date+ timedelta(days=1)   
        FirstDate = Sdate- timedelta(days=data_window*2)
        logging.info(f'{sym} Daily Prediction parameters = FirstDate: {FirstDate},  Sdate: {Sdate},   mktdate:{mktdate}')
        if mktdate >= Sdate:
            symdf = DU.load_df(stock_symbol=sym, startdt=FirstDate, lastdt=mktdate, DailyMode=True, dataMode="P")
            symdf["Date"] =  pd.to_datetime(symdf["Date"]).dt.date
            if len(symdf) <= data_window:
                return
            mktdate2 = symdf.iloc[-1]['Date']
            if mktdate2 < Sdate:
                return
            x_arr, y_arr, outdf = dgm.prepare_input_data(symdf, data_window, startdt=Sdate)

            # load the model for prediction
            mpath = f'./model/{sym}.h5'
            if not path.exists(mpath):
                logging.error(f'Model file {mpath} is not existed.')
                return

            model = keras.models.load_model(mpath)
            Y_pred = []

            # loop over range of dates
            for i in range(x_arr.shape[0]):
                dt = outdf.iloc[i]['Date']
                X = x_arr[i].reshape(1, x_arr[i].shape[0], x_arr[i].shape[1])
                pred = model.predict(X)
                Y = pred.flatten()
                # print(dt, '  = ', pred, 'P.shape= ', pred.shape,  'Y= ', Y, ' Y.shape=', Y.shape)
                Y_pred.append(Y[0])
            Y_pred = np.array(Y_pred)
            logging.debug(f'Y_pred: {Y_pred.shape}')
            outdf['Ypred'] = Y_pred
            # predict a range of dates
            Y_pred = model.predict(x_arr)
            outdf['Ypred-L'] = Y_pred
            #
            if lastpred is None:
                predClose = outdf['Close'] + outdf['Ypred']
                outdf['predClose'] = predClose   
            else:
                predClose = np.insert((outdf['Close'] + outdf['Ypred']).values, 0, lastpred.predClose)
                outdf['predClose'] = predClose[1:]

            # try different log returns calculations
            outdf['logRetDF1'] = np.log(outdf.predClose) - np.log(outdf.predClose.shift(1))
            outdf['logRetDF2'] = np.log(outdf.predClose/outdf.predClose.shift(1))
            if lastpred is None:
                outdf['logRet'] = np.insert(np.diff(np.log(predClose)), 0, 0.0)
            else:
                outdf['logRet'] = np.diff(np.log(predClose))

            outdf = outdf.reset_index().drop(columns=['index'])
            fpath = f'./gmodel_predict/{sym}_{Sdate}_{mktdate}.csv'
            outdf.to_csv(fpath, index=False)
            outdf = outdf[['Date','Symbol','Exchange','YTest','Ypred','Ypred-L','predClose','logRet']]
            DU.StoreEOD(outdf, DBPREDICT, TBLDLYPRED)

    DU.setDBSSH()
    # main procedure start here
    # add code to handle multiply stock code lists
    #
    lastMktDate = DU.get_Max_date(f"{DBMKTDATA}.{TBLDAILYPRICE}")
    logging.debug(f'Last Market data date {lastMktDate}')
    #
    # set flag to True is only do prediction 
    #
    predictOnly = False

    code_list = ["etf_list", "stock_list", "crypto_list", "us-cn_stock_list"]
    # code_list = ['test_list']
    for lname in code_list:
        symbol_list = DU.get_Symbollist(lname)
        logging.info(symbol_list)

        for symbol in symbol_list:
            Process_Prediction(symbol, lastMktDate)
            # for debug only
            # if processCount > 6:
            #     break
            # else:
            #     processCount += 1
            
    lastPredictdt = DU.get_Max_date(f"{DBPREDICT}.{TBLDLYPRED}")
    logging.debug(f'Last Prediction date {lastPredictdt}')

    predictDF = DU.load_df_SQL(f"select * from {DBPREDICT}.{TBLDLYPRED} where date = \'{lastPredictdt}\'")
    logging.debug(f'predictDF.size: {predictDF.shape}')

    def setPredictionString(y_predict):
        if y_predict < 0.0:
            return 'DOWN'
        elif y_predict > 0.0:
            return 'UP'
        else:
            return ' '

    if len(predictDF) > 0 and not predictOnly:
        # symlist = data.load_symbols("web_stock_list")
        symlist = DU.get_Symbollist("web_stock_list")
        logging.debug(f'Web display list : {symlist}')
        symlist2 = list(set(symlist) & set(list(predictDF.Symbol.values)))
        ret = predictDF.set_index('Symbol').loc[symlist2].reset_index(inplace=False)
        predictDF = ret.set_index('Date').reset_index()
        predictDF['Prediction'] = predictDF.Ypred.apply(setPredictionString)
        predictDF['Accuracy'] = 0
        # remove column 'Accuracy' for production web site
        displayDF = predictDF[['Date','Symbol','Exchange','Prediction']]
        displayDF.to_csv(f'weboutput/gmodel_{lastPredictdt}.csv', index=False)

        # refresh the web table and upload the latest prediction
        DU.StoreWebDaily(displayDF)
