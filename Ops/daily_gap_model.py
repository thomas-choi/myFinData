import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
# from sklearn import preprocessing
import numpy as np
# from finta import TA
import keras
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, Dropout, LSTM, Input, Activation
from tensorflow.keras import optimizers
# from keras.callbacks import History 
from sklearn.metrics import mean_squared_error
from dotenv import load_dotenv
from os import path
from os import environ
import logging
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
from data import processing

test_limit = 504
data_window = 21
train_split = 0.7   

def prepare_input_data(data_df, input_days, startdt=None):
    # features: Adj_Close
    logging.info(f'prepare_input_data({input_days}):\n data_df shape: {data_df.shape},   startdt: {startdt}')
    logging.debug(f'in_df.head(1):\n{data_df.head(1)}')
    logging.debug(f'in_df.tail(1):\n{data_df.tail(1)}')
    d_arr = np.diff(data_df.loc[:, ['AdjClose']].values, axis=0)
    x_arr = np.array([d_arr[i : i+input_days] for i in range(len(d_arr) - input_days)])
    y_arr = np.array([d_arr[i + input_days] for i in range(len(d_arr)-input_days)])
    logging.debug(f'd_arr.size = {d_arr.shape}')
    logging.debug(f'x_arr.size = {x_arr.shape}')
    logging.debug(f'y_arr.size = {y_arr.shape}')
    out_df = data_df.iloc[input_days:].reset_index().drop(columns=['index'])
    logging.debug(f'out_df.head(1):\n{out_df.head(1)}')
    logging.debug(f'out_df.tail(1):\n{out_df.tail(1)}')
    logging.debug(f'out_df.size:   {out_df.shape}')
    
    # filter by start date
    if startdt is not None:
        out_df = out_df[out_df['Date']>= startdt].reset_index()
        logging.debug(f'out_df.head(1):\n{out_df.head(1)}')
        logging.debug(f'out_df.tail(1):\n{out_df.tail(1)}')
        logging.debug(f' out_df.size:    {out_df.shape}')
        logging.debug(f'startdt: {startdt}  <->   out_df.Date[0]: {out_df.Date[0]}')
        logging.debug(f' xarry[0] :  {x_arr[0][:3]}')
        logging.debug(f' yarry[0] :  {y_arr[0]}    d[0]: {d_arr[0]}')
        logging.debug(f' xarry[1] :  {x_arr[1][:3]}')
        logging.debug(f' yarry[1] :  {y_arr[1]}    d[1]: {d_arr[1]}')
        startidx = out_df['index'][0]-1
        if startidx < 0:
            startidx = 0
            out_df = out_df[1:]
        logging.debug(f'start Idx: {startidx}')
        out_df = out_df.drop(columns=['index'])
        x_arr = x_arr[startidx:,:]
        y_arr = y_arr[startidx:,:]
        logging.debug(f' new y_arr.size:    {y_arr.shape}')
        out_df['YTest'] = y_arr
    return x_arr, y_arr, out_df

def train_test_split_preparation(new_df, input_days, train_split):
    new_df = new_df.loc[1:]

    #Preparation of train test set.
    test_num =  min(int(new_df.shape[0] * (1.0 - train_split)), test_limit)
    # train_indices = int(new_df.shape[0] * train_split)
    train_indices = int(new_df.shape[0] - test_num)
    logging.info(f'Training sample #: {train_indices}           Testing sample #: {test_num}')    

    train_data = new_df[:train_indices]
    test_data = new_df[train_indices:]
    test_data = test_data.reset_index()
    test_data = test_data.drop(columns = ['index'])
    fdt = train_data.iloc[0, train_data.columns.get_loc('Date')]
    edt = train_data.iloc[-1, train_data.columns.get_loc('Date')]
    logging.info(f'Traing data from {fdt} to {edt}')
    fdt = test_data.iloc[0, test_data.columns.get_loc('Date')]
    edt = test_data.iloc[-1, test_data.columns.get_loc('Date')]
    logging.info(f'Testing data from {fdt} to {edt}')
    
    X_train, y_train, train_outdf = prepare_input_data(train_data, input_days)
    X_test, y_test, test_outdf = prepare_input_data(test_data, input_days)

    ref_date = test_data['Date'][input_days]
    logging.info(f'Testing date from {ref_date}')
    
    return X_train, y_train, X_test, y_test, test_data

def lstm_model(X_train, y_train, input_days):
    #Setting of seed (to maintain constant result)
    tf.random.set_seed(20)
    np.random.seed(10)

    lstm_input = Input(shape=(input_days, 1), name='input_for_lstm')

    inputs = LSTM(input_days, name='first_layer', return_sequences = True)(lstm_input)

    inputs = Dropout(0.1, name='first_dropout_layer')(inputs)
    inputs = LSTM(32, name='lstm_1')(inputs)
    inputs = Dropout(0.05, name='lstm_dropout_1')(inputs) #Dropout layers to prevent overfitting
    inputs = Dense(32, name='first_dense_layer')(inputs)
    inputs = Dense(1, name='dense_layer')(inputs)
    output = Activation('linear', name='output')(inputs)

    model = Model(inputs=lstm_input, outputs=output)
    adam = optimizers.Adam(learning_rate = 0.002)

    model.compile(optimizer=adam, loss='mse')
    model.fit(x=X_train, y=y_train, batch_size=15, epochs=25, shuffle=True, validation_split = 0.1)

    return model

# def predict_stock(backDate, dailydf=None):

