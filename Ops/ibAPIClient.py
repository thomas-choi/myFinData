from ibapi.wrapper import *
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import SetOfString
from ibapi.common import SetOfFloat
from threading import Timer
import logging
from datetime import datetime

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# use the below IB API document as reference
# https://interactivebrokers.github.io/tws-api/tick_types.html

type_keep = {1,2,4,8,9,21,22,23,24,27,28,29,30}

class IBClient (EWrapper, EClient):

    def __init__ (self, contract_list, contract_quotes):
        EClient.__init__ (self, self)
        self.c_list = contract_list
        self.c_quotes = contract_quotes

    @staticmethod
    def create_opt_quote():
        opt_quote = dict()
        opt_quote['Symbol'] = ''
        opt_quote['PnC'] = ''
        opt_quote['Strike'] = 0.0
        opt_quote['Expiration'] = ''
        opt_quote['lastPrice'] = 0.0
        opt_quote['ask'] = 0.0
        opt_quote['bid'] = 0.0
        opt_quote['change'] = 0.0
        opt_quote['percentChange'] = 0.0
        opt_quote['volume'] = 0.0
        opt_quote['openInterest'] = 0.0
        opt_quote['impliedVolatility'] = 0.0
        opt_quote['inTheMoney'] = ''
        opt_quote['contractSize'] = 0
        opt_quote['currency'] = ''
        opt_quote['PClose'] = 0.0
        opt_quote['undPrice'] = 0.0
        opt_quote['timestamp'] = ''
        return opt_quote
    
    def get_quote(self, reqId):
        if reqId not in self.c_quotes:
            self.c_quotes[reqId] = IBClient.create_opt_quote()
        return self.c_quotes[reqId]

    def error (self, reqId, errorCode, errorString, advancedOrderRejectJson):
        if advancedOrderRejectJson:
            print ("Error: ", reqId, " ", errorCode, " ", errorString, ", AdvancedOrderRejectJson:", advancedOrderRejectJson)
        else:
            print ("Error: ", reqId, " ", errorCode, " ", errorString)

    def tickPrice(self, reqId, tickType, price, attrib): 
        t_type_str = TickTypeEnum.to_str(tickType)
        logging.info(f"tickPrice reqId: {reqId}, type={tickType}.{t_type_str}, price={price}, CanAutoExecute:{attrib.canAutoExecute}, PastLimit:{attrib.pastLimit}")
        if tickType in type_keep:
            quote = self.get_quote(reqId)
            if tickType == 1:
                quote['bid'] = price
            elif tickType == 2:
                quote['ask'] = price
            elif tickType == 4:
                quote['lastPrice'] = price
                quote['change'] = quote['lastPrice'] - quote['PClose']
            elif tickType == 9:
                quote['PClose'] = price
                quote['change'] = quote['lastPrice'] - quote['PClose']
            logging.info(f'quote price({reqId}):{quote}')

    def tickSize(self, reqId, tickType, size):
        t_type_str = TickTypeEnum.to_str(tickType)
        logging.info(f"tickSize reqId: {reqId}, type={tickType}.{t_type_str}, size={size}")
        if tickType in type_keep:       
            quote = self.get_quote(reqId)
            if tickType == 8:
                quote['volume'] = size
            elif (tickType == 27) and (quote['PnC'] == 'C'):
                quote['openInterest'] = size
            elif (tickType == 28) and (quote['PnC'] == 'P'):
                quote['openInterest'] = size
            logging.info(f'quote size({reqId}):{quote}')
    
    def tickOptionComputation (self, reqId, tickType, tickAttrib, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        type_s = TickTypeEnum.to_str(tickType)
        s=f"TickOptionComputation. reqID:{reqId}, tickType:{tickType}.{type_s}, tickAttrib:{tickAttrib}, ImpliedVolatility:{impliedVol}, Delta:{delta},\
            OptionPrice:{optPrice}, pvDividend:{pvDividend}, Gamma:{gamma}, Vega:{vega}, Theta:{theta}, UnderlyingPrice:{undPrice}"
        logging.info(s)

        # use option computation based on last price(12) or model (13)
        if tickType == 12:
            # prefix = type_s.split("_")[0]
            quote = self.get_quote(reqId)
            # quote[prefix+"_"+'tickAttrib'] = tickAttrib
            quote['impliedVolatility'] = impliedVol
            # quote['delta'] = delta
            # quote[prefix+'_'+'optPrice'] = optPrice
            # quote[prefix+'_'+'pvDividend'] = pvDividend
            # quote['gamma'] = gamma
            # quote['vega'] = vega
            # quote['theta'] = theta
            quote['undPrice'] = undPrice
            logging.info(f'OptionsComp:{reqId}: {quote}')

    def tickGeneric (self, reqId, tickType, value):
        type_s = TickTypeEnum.to_str(tickType)
        logging.info(f"Tick Generic. reqId:{reqId}, tickType:{tickType}.{type_s}, Value:{value}")
        # print(f"Tick Generic. reqId:{reqId}, tickType:{tickType}.{type_s}, Value:{value}")

    # def nextValidId (self, orderId):
    #     print(f"app.nextValidID: {orderId}")
    #     self.start ()

    def securityDefinitionOptionParameter (self, reqId:int, exchange:str, underlyingConId:int, tradingClass:str, multiplier:str, 
                                           expirations:SetOfString, strikes:SetOfFloat):
        istr = f"SecurityDefinitionOptionParameter. ReqId:{reqId}, Exchange:{exchange}, Underlying conId:{underlyingConId}, TradingClass:{tradingClass}, Multiplier:{multiplier}, Expirations:{expirations}, Strikes:{str (strikes),}\n"
        logging.info(istr)

    def securityDefinitionOptionParameterEnd (self, reqId:int):
        istr= f"SecurityDefinitionOptionParameterEnd. ReqId:{reqId}"
        logging.info(istr)

    # def start (self):
    # # 265598 is the conId for AAPL Nasdaq stock
    #     print("App.start()")
    #     self.reqSecDefOptParams (1, "AAPL", "", "STK", 265598)
        
    def stop (self):
        print("App.stop()")
        self.done = True
        self.disconnect ()

def Option_contracts():
    # Define an Option contract object
    contracts = []

    contract = Contract()
    contract.symbol = "EFA" # Change the symbol as needed
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.secType = "OPT"
    contract.lastTradeDateOrContractMonth = "20240223" # Change the expiration date as needed
    contract.strike = 75 # Change the strike as needed
    contract.right = "C" # Change the right as needed
    contracts.append(contract)

    contract = Contract()
    contract.symbol = "SLV" # Change the symbol as needed
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.secType = "OPT"
    contract.lastTradeDateOrContractMonth = "20240328" # Change the expiration date as needed
    contract.strike = 22 # Change the strike as needed
    contract.right = "P" # Change the right as needed
    contracts.append(contract)
    return contracts

def Stock_contract():
    contract = Contract()
    contract.symbol = "IBM"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract

if __name__ == "__main__":
    logging.basicConfig(filename=f'IBAPI{datetime.today().date()}.log', filemode='a', 
                        format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                        level=logging.INFO)

    # Create an IBClient object
    opt_contracts_lst = Option_contracts()
    opt_quotes = dict()
    client = IBClient(opt_contracts_lst, opt_quotes)
        
    client.nextOrderId = 0
    # TWs 7497, IBGW 4001
    gwip = '172.27.96.1'
    # gwip = "192.168.11.111"
    gwport = 7496
    print("App.connect()")
    client.connect(gwip, gwport, 1)
    # print("Timer.start()")
    # Timer (4, app.stop).start ()
    for i in range(len(opt_contracts_lst)):
        client.reqMktData(i, opt_contracts_lst[i], "", False, False, []) # The genericTickList parameter is "100" to get the option price

    client.run ()
