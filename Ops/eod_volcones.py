import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from os import path
from dotenv import load_dotenv
import logging
from os import environ
import yfinance as yf
import argparse

import Volatility as flVol
import dataUtil as DU

windows = [30, 60, 90, 120]
quantiles = [0.25, 0.75]

def Plot_V_Cone_afig(res, win):
    # create the plots on the chart

    plt.plot(win, res["min"], "-o", linewidth=1, label="Min")
    plt.plot(win, res["max"], "-o", linewidth=1, label="Max")
    plt.plot(win, res["median"], "-o", linewidth=1, label="Median")
    plt.plot(win, res["top_q"], "-o", linewidth=1, label=f"{quantiles[1] * 100:.0f} Prctl")
    plt.plot(win, res["bottom_q"], "-o", linewidth=1, label=f"{quantiles[0] * 100:.0f} Prctl")
    plt.plot(win, res["realized"], "ro-.", linewidth=1, label="Realized")

    # set the x-axis labels
    plt.xticks(win)
    if 'title' in res:
        plt.title(res['title'])
    else:
        plt.title(f'{res["symbol"]} Volatility Cones')
    # format the legend
    plt.legend(loc="upper center", ncol=3)

def realized_vol(price_data, window=30):

    log_return = (price_data["Close"] / price_data["Close"].shift(1)).apply(np.log)

    return log_return.rolling(window=window, center=False).std() * math.sqrt(252)

Vol_method = 'yang_zhang'

def Volality_Cone(symbol, data):
    min_ = []
    max_ = []
    median = []
    top_q = []
    bottom_q = []
    realized = []
    for window in windows:
        # get a dataframe with realized volatility
        estimator = flVol.yang_zhang(window=window, price_data=data)

        # append the summary stats to a list
        min_.append(estimator.min())
        max_.append(estimator.max())
        median.append(estimator.median())
        top_q.append(estimator.quantile(quantiles[1]))
        bottom_q.append(estimator.quantile(quantiles[0]))
        realized.append(estimator[-1])
    vol_ret = dict()
    vol_ret["symbol"] = symbol
    vol_ret["min"] = min_
    vol_ret["max"] = max_
    vol_ret["median"] = median
    vol_ret["top_q"] = top_q
    vol_ret["bottom_q"] = bottom_q
    vol_ret["realized"] = realized
    return vol_ret

def Plot_by_list_afig(list_n, _list, startdt, enddt):
    vc_dir=environ.get("VOL_CON_DIR")
    for sym in s_list:
        # DF = yf.download(sym, start=startdt, end=enddt)
        # print('from yahoo.financie:\n\n', DF.head())
        DF = DU.load_eod_price(sym, start=startdt, end=enddt).set_index('Date')
        print(DF.head())
        result = Volality_Cone(sym, DF)
        result['title'] =f'{sym} Volatility Cones from {startdt} to {enddt}'
        Plot_V_Cone_afig(result, windows)
        plt.savefig(f'{vc_dir}/{sym}_Volatility_Cones.pdf')
        # plt.show(block=False)
        plt.clf()

def Plot_V_Cone(ax, res, win):
    # create the plots on the chart

    ax.plot(win, res["min"], "-o", linewidth=1, label="Min")
    ax.plot(win, res["max"], "-o", linewidth=1, label="Max")
    ax.plot(win, res["median"], "-o", linewidth=1, label="Median")
    ax.plot(win, res["top_q"], "-o", linewidth=1, label=f"{quantiles[1] * 100:.0f} Prctl")
    ax.plot(win, res["bottom_q"], "-o", linewidth=1, label=f"{quantiles[0] * 100:.0f} Prctl")
    ax.plot(win, res["realized"], "ro-.", linewidth=1, label="Realized")

    # set the x-axis labels
    ax.set_xticks(win)
    if 'title' in res:
        ax.set_title(res['title'])
    else:
        ax.set_title(f'{res["symbol"]} Volatility Cones')
    # format the legend
    ax.legend(loc="upper center", ncol=3)

def Plot_by_list_combined(list_n, _list, startdt, enddt, rows, cols, width, hight):
    print(f'rows={rows}, cols={cols}')
    print(f'width={width}, hight={hight}')
    fig, ax = plt.subplots(rows, cols, figsize=(width,hight))
    row = 0
    col = 0

    for sym in s_list:
        fp = f'data/{sym}.csv'
    #     print(f'Data file path: {fp}')
        if path.isfile(fp):
            DF = pd.read_csv(fp).set_index('Date')
        else:
            DF = yf.download(sym, start=startdt, end=enddt)
            DF.reset_index().to_csv(fp, index=False)
        result = Volality_Cone(sym, DF)
        print(f'Plot {row},{col}')
        result['title'] =f'{sym} Volatility Cones from {startdt} to {enddt}'
        Plot_V_Cone(ax[row,col], result, windows)
        col += 1
        if col >= cols:
            row += 1
            col = 0
    plt.savefig(f'{Vol_method}/{list_n}_Vol_{startdt}_{enddt}.png')
    plt.show()

if __name__ == '__main__':
    load_dotenv("../Prod_config/Stk_eodfetch_PythonAnywhere.env") #Check path for env variables
    logging.basicConfig(filename=f'logging/eod_volcones{datetime.today().date()}.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    tzNow = DU.nowbyTZ('US/Eastern')

    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--Date', dest='InDate', type=str)
    parser.add_argument('-S', '--SSHDB', dest='SSHDB', action='store_true', default=False)
    parser.add_argument('-t', '--test', dest='test', action='store_true', default=False)
    parser.add_argument('-c', '--check', dest='checkFlag', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='forceFlag', action='store_true', default=False)

    args = parser.parse_args()

    logging.debug(f'argments: {args}')
    if args.InDate is not None:
        todt = datetime.strptime(args.InDate, '%Y-%m-%d').date()
    else:
        todt = tzNow.date()

    all_lists = ['etf_list','stock_list','us-cn_stock_list']
    if args.test:
        all_lists=['test_list']
    enddt = todt - timedelta(days = 1)
    startdt = enddt - timedelta(days = 365)
    cols = 4
    plt_dim = 7
    if not (todt.isoweekday() in range(1,6) or args.forceFlag):
        print('Jobs must be ran from Monday to Friday. use -f otherwise')
        quit()
    logging.info(f'Process volatility cones from {startdt} to {enddt} on {all_lists}.')
    if args.SSHDB:
        DU.setDBSSH()
    if args.checkFlag:
        quit()

    # remove data/*.csv before run this
    for lname in all_lists:
        logging.info(f'Processing {lname}....')
        s_list = DU.get_Symbollist(lname)
        rows = math.ceil(len(s_list)/cols)
        # Plot_by_list_combined(lname, s_list, startdt, enddt, rows, cols, plt_dim*cols, plt_dim*rows)
        Plot_by_list_afig(lname, s_list, startdt, enddt)

