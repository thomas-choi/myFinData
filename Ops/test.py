from eoddata_fetch import get_Max_date
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
import time
import pytz

load_dotenv("../Prod_config/Stk_eodfetch_PythonAnywhere.env") #Check path for env variables
logging.getLogger().setLevel(logging.DEBUG)

def DBTest():

    mkdate = get_Max_date('twmchoi2022$GlobalMarketData.histdailyprice6')

    print(mkdate)

tzinfo = time.tzname
logging.info(f'TimeZone info: {tzinfo[0]}\n\n')

tformats = '%Y-%m-%d %H:%M:%S'

tNow = datetime.now()
typeNow = type(tNow)
logging.debug(f'Type of tNow:{typeNow}')
today5PM = tNow.replace(hour=17, minute=0, second=0, microsecond=0)
logging.info(f'Current Date-Time is {tNow}, and cutt-off time is {today5PM}')

mToday = tNow.date()
logging.info(f'mToday is {mToday}')
if tNow < today5PM:
    mToday = mToday - timedelta(days=1)
logging.info(f'mToday after cutoff is {mToday}\n\n')

estNow = datetime.strptime(tNow.astimezone(pytz.timezone('US/Eastern')).strftime(tformats), tformats)
logging.info(f'Current Date-Time is {estNow}')
typeNow = type(estNow)
logging.debug(f'Type of estNow:{typeNow}')
today5PM = estNow.replace(hour=17, minute=0, second=0, microsecond=0)
logging.info(f'     and cutt-off time is {today5PM}')

estToday = estNow.date()
logging.info(f'estToday is {estToday}')
if estNow < today5PM:
    estToday = estToday - timedelta(days=1)
logging.info(f'mToday after cutoff is {estToday}\n\n')

# my_datetime_est = datetime.today().astimezone(pytz.timezone('US/Eastern'))
# print(f'mToday in EST is {my_datetime_est}')

# my_datetime_est = datetime.today().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
# print(f'mToday in EST is {my_datetime_est}')

# my_datetime_est = datetime.today().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S %Z%z')
# print(f'mToday in EST is {my_datetime_est}')