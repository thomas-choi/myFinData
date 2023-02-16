import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import yfinance as yf
import os

from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
from statsmodels.tsa.seasonal import seasonal_decompose, STL
import plotly.graph_objects as go
import plotly.io as pio
import argparse

import dataUtil as DU

disp_limit = 0.5

def OptStrikes(df, title, cStrikeList, pStrikeList, cLimit, pLimit, htmlfile=''):
    fig = go.Figure(data=[go.Candlestick(x=df['Date'],
                    open=df['Open'],
                    high=df['High'],
                    low=df['Low'],
                    close=df['Close'])])
    # 1st strike is large width line
    if cStrikeList[0] <= cLimit:
        fig.add_hline(y=cStrikeList[0], line_color="green", line_width=5,
                    annotation_text=f"+1 {cStrikeList[0]} CALL", annotation_font_size=30)
    for i in range(1, len(cStrikeList)):
        if cStrikeList[i] <= cLimit:
            fig.add_hline(y=cStrikeList[i], line_color="green", line_width=2, line_dash="dash",
                        annotation_text=f"+{i+1} {cStrikeList[i]} CALL", annotation_font_size=30)
    if cStrikeList[0] >= pLimit:
        fig.add_hline(y=pStrikeList[0], line_color="red", line_width=5,
                    annotation_text=f"-1 {pStrikeList[0]} PUT", annotation_font_size=30)
    for i in range(1, len(pStrikeList)):
        if cStrikeList[i] >= pLimit:
            fig.add_hline(y=pStrikeList[i], line_color="red", line_width=2, line_dash="dash",
                        annotation_text=f"-{i+1} {pStrikeList[i]} PUT", annotation_font_size=30)
    # fig.add_hline(y=163, line_color="red", line_width=5, annotation_text="163 PUT", annotation_font_size=30)
    fig.update_layout(title_text=title, title_x=0.5,
                     width=900,
                     height=800,
                     margin=dict(l=30,r=30,b=30,t=50),
                    paper_bgcolor="LightSteelBlue",
                      xaxis_rangeslider_visible=False)
    if len(htmlfile)>0:
#         fig.write_html(f'{htmlfile}.html')
        pio.write_image(fig, f'{htmlfile}.pdf')
    # fig.show()
    plt.clf()

def ConvertWeekly(inDF):
    logic = {'Open'  : 'first',
         'High'  : 'max',
         'Low'   : 'min',
         'Close' : 'last',
         'AdjClose': 'last',
         'Volume': 'sum'}

    df = inDF.resample('W').apply(logic)
    df.index = df.index - pd.tseries.frequencies.to_offset("6D")
    return df

def ProcessTickerOpChart(dailyDF, weeklyDF, ticker, stk_num):
    vc_dir=os.environ.get("VOL_CON_DIR")

    maxDate, sec = DU.get_Max_Options_date('twmchoi2022$GlobalMarketData.OptionChains', symbol=ticker)
    logging.debug(f'MaxDate: {maxDate}, Section:{sec}')
    query=f"SELECT * FROM twmchoi2022$GlobalMarketData.OptionChains where UnderlyingSymbol = \'{ticker}\' and \
    Date=\'{maxDate}\' and section = \'{sec}\' and strike < UnderlyingPrice and OptionType = \'put\' order by openInterest desc \
    limit {stk_num};"
    logging.debug(query)
    putDF = DU.load_df_SQL(query)
    putDF.head()
    Cquery=f"SELECT * FROM twmchoi2022$GlobalMarketData.OptionChains where UnderlyingSymbol = \'{ticker}\' and \
    Date=\'{maxDate}\' and section = \'{sec}\' and strike > UnderlyingPrice and OptionType = \'call\' order by openInterest desc \
    limit {stk_num};"
    logging.debug(Cquery)
    callDF = DU.load_df_SQL(Cquery)
    title = f'{ticker} top {stk_num} strikes-Daily'
    CallS, PutS = callDF.strike.values, putDF.strike.values
    CLimit = (1+disp_limit) * callDF.UnderlyingPrice.values[0]
    PLimit = (1-disp_limit) * putDF.UnderlyingPrice.values[0]
    logging.debug(f'Call strikes {CallS} and limit {CLimit}')
    logging.debug(f'Put strikes {PutS} and limit {PLimit}')
    # OptStrikes(dailyDF.reset_index(), title, CallS, PutS, "")
    title = f'{ticker} top {stk_num} strikes-Weekly'
    filepath = os.path.join(vc_dir, f'{ticker}_top_{stk_num}_strikes-Weekly')
    OptStrikes(weeklyDF.reset_index(), title, CallS, PutS, CLimit, PLimit, filepath)

if __name__ == '__main__':
    load_dotenv("../Prod_config/Stk_eodfetch.env") #Check path for env variables
    logging.basicConfig(filename=f'logging/eod_optStrikes{datetime.today().date()}.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    tzNow = DU.nowbyTZ('US/Eastern')

    parser = argparse.ArgumentParser()
    parser.add_argument('-D', '--Date', dest='Date', type=str)
    parser.add_argument('-n', '--Num', dest='topNum', type=int, default=4)
    parser.add_argument('-S', '--SSHDB', dest='SSHDB', action='store_true', default=False)
    parser.add_argument('-t', '--test', dest='test', action='store_true', default=False)
    parser.add_argument('-c', '--check', dest='checkFlag', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='forceFlag', action='store_true', default=False)

    args = parser.parse_args()

    logging.debug(f'argments: {args}')
    if args.Date is not None:
        todt = datetime.strptime(args.Date, '%Y-%m-%d').date()
    else:
        todt = tzNow.date()

    topnum=args.topNum
    numOfdays=170
    all_lists = ['etf_list','stock_list','us-cn_stock_list']
    if args.test:
        all_lists=['test_list']
    enddt = todt - timedelta(days = 1)
    startdt = enddt - timedelta(days = 3*numOfdays)
    if not (todt.isoweekday() in range(1,6) or args.forceFlag):
        print('Jobs must be ran from Monday to Friday. use -f otherwise')
        quit()
    logging.info(f'Process Top {topnum} strikes from {startdt} to {enddt} on {all_lists}.')
    if args.SSHDB:
        DU.setDBSSH()
    if args.checkFlag:
        quit()

    for listN in all_lists:
        symlist = DU.get_Symbollist(listN)
        for ticker in symlist:
            logging.debug(f'ticker {ticker} in {listN}')
            # sDF = yf.download(ticker,startdt,enddt)
            DF = DU.load_eod_price(ticker, start=startdt, end=enddt)
            DF['Date'] = pd.to_datetime(DF['Date'])
            DF = DF.set_index('Date')
            sDF = DF[['Open','High','Low','Close','AdjClose','Volume']]
            logging.debug(sDF.head())
            wkDF = ConvertWeekly(sDF)
            logging.debug(wkDF)
            ProcessTickerOpChart(sDF, wkDF, ticker, topnum)
