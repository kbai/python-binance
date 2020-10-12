import json
import time
from datetime import datetime,timedelta
import threading
import numpy as np
import pandas as pd
import zmq
import logging

from binance.websockets import BinanceSocketManager
from marketMaker.OrderManager import *
from marketMaker.PortfolioManager import *
from util import *


class Signal:

    def __init__(self):
        self.seqNum = 0
        self.action = Action.NOACTION
        self.signalPrice = 0.0

    def update(self, action, price):
        self.action = action
        self.seqNum += 1
        self.price = price


signal = Signal()


def order_handling():
    lastSeqNum = -1
    while True:
        if lastSeqNum < signal.seqNum:
            lastSeqNum = signal.seqNum
            if signal.action == Action.NOACTION:
                continue
            if signal.action == Action.BUY:
                if pm.getPosition('BNB') >= 1.0:
                    continue


keys = pd.read_csv('./tradingkey.csv')
print(keys)
print(keys['key'][4], keys['key'][5])

context = zmq.Context()
client = context.socket(zmq.SUB)
client.connect(connEndPoint)
client.setsockopt_string(zmq.SUBSCRIBE, connTopic)

cmd = context.socket(zmq.SUB)
cmd.connect(commandEndPoint)
cmd.setsockopt_string(zmq.SUBSCRIBE, commandTopic)


def monitorParams():
    while True:
        try:
            rcv = cmd.recv_string()
            print(rcv)
            _, data = rcv.split(commandTopic)
            res = json.loads(data)
            print(res)
            params.update({k: float(res[k]) for k in res})
            print(params)
        except Exception as e:
            print(e)


cmdThread = threading.Thread(target = monitorParams)

cmdThread.start()
tc = Client(keys['key'][4], keys['key'][5])

#tc = Client(keys['key'][0], keys['key'][1], tld='us') #if using binance.us

pm = PortfolioManager()
bm = BinanceSocketManager(tc)

#bm = BinanceSocketManager(tc, context = 'us')  #if using binance.us
om = OrderManager(tc)

params = {'ready': 0,
          'posUpperLimit': 0,
          'posLowerLimit': 0,
          'spread': 10.1,
          'buysellSkew': 0.0,
          'alphaMultiplier': 0.0,
          'positionSkew': 0.0}


def processmymsg(msg: dict):
    print(msg)
    if msg.get('e', '') == 'outboundAccountPosition':
        pm.processPositionUpdate(msg)
        return
    if msg.get('e', '') == 'executionReport':
        print(msg['x'], msg['s'], msg['S'])
        om.processOrderUpdate(msg)
        return
    return


bm.start_user_socket(processmymsg)
bm.start()
# wait for binance user data feed to ready
time.sleep(2)



class ewma:
    def __init__(self, period):
        self.value = 0.0
        self.decay = np.exp(-1. / period)
        self.init = 1

    def update(self, cv):
        self.value = self.value * self.decay + (1 - self.decay) * cv + self.init * self.decay * cv
        self.init = 0


class LastTradeManager:
    def __init__(self):
        self.lt = {}

    def update(self, symbol, value):
        self.lt[symbol] = value

    def get(self, symbol):
        return self.lt[symbol]


class EwmaManager:
    def __init__(self):
        self.ewmapool = {}

    def register(self, symbol, period):
        self.ewmapool.setdefault(symbol, {})
        self.ewmapool[symbol].setdefault(period, ewma(period))

    def updateSymbol(self, symbol, period, value):
        try:
            self.ewmapool[symbol][period].update(value)
        except:
            print("not found ")

    def updateSymbolAll(self, symbol, value):
        # try:
        for ema in self.ewmapool[symbol]:
            self.ewmapool[symbol][ema].update(value)

    # except:
    # print("not found ")
    def getValue(self, symbol):
        return [(x, self.ewmapool[symbol][x].value) for x in self.ewmapool[symbol]]

    def getValue(self, symbol, period):
        return self.ewmapool[symbol][period].value


ewmaManager = EwmaManager()
ewmaManager.register('ETHUSDT', 100)
ewmaManager.register('ETHUSDT', 500)
ewmaManager.register('ETHUSDT', 500)
ewmaManager.register('ETHUSDT', 1000)

ewmaManager.register('BTCUSDT', 100)
ewmaManager.register('BTCUSDT', 500)
ewmaManager.register('BNBUSDT', 100)
ewmaManager.register('BNBUSDT', 10)

ewmaManager.register('BNBUSDT', 1000)

ewmaManager.register('BNBUSDT', 500)
ewmaManager.register('LTCUSDT', 1)
ewmaManager.register('BNBUSDT2', 100)
ewmaManager.register('BNBUSDT2', 10)
ewmaManager.register('SIGNAL', 100)

# time.sleep(10000);


lastTradeManager = LastTradeManager();
pos = tc.get_asset_balance(asset='BNB', recvWindow=10000)
pm.positions['BNB'] = float(pos['free']) + float(pos['locked'])


def aftertrade():
    print("trade")


def getReturn(symbol, period):
    return np.log(lastTradeManager.get(symbol) / ewmaManager.getValue(symbol, period))


print(tc.get_asset_balance(asset='ETH', recvWindow=10000))
print(tc.get_account_status(recvWindow=10000))
noExistingOrder = True
lastorder = {}
luap = lubp = time.time_ns()
vol = 0.0


def updateBidAsk(res):
    #          print(res['bids'][0])
    ewmaManager.updateSymbolAll('ETHUSDT', lastTradeManager.get('ETHUSDT'))
    #           print(ewmaManager.getValue('ETHUSDT'))
    #           print(ewmaManager.getValue('BNBUSDT'))
    bid = float(res['bids'][0][0])
    ask = float(res['asks'][0][0])
    smid = 0.5 * (bid + ask)
    ewmaManager.updateSymbolAll('BNBUSDT', smid)
    ewmaManager.updateSymbolAll('BNBUSDT2', smid ** 2)

    vol = np.sqrt(ewmaManager.getValue('BNBUSDT2', 100) - ewmaManager.getValue('BNBUSDT', 100) ** 2)
    print('volatility: {:.2f}'.format(vol))
    print(res['lastUpdateId'])
    print(','.join(['{:.4f}']*7).format(bid, ask, smid,
                                                                    getReturn('ETHUSDT', 100) * 100,
                                                                    getReturn('ETHUSDT', 500) * 100,
                                                                    getReturn('BNBUSDT', 100) * 100,
                                                                    getReturn('BNBUSDT', 500) * 100))

    signal = 100 * (0.008 * getReturn('BNBUSDT', 100) - 0.2863 * getReturn('BNBUSDT', 500) - 0.0177 * getReturn(
        'BNBUSDT', 1000)
                    - 0.3832 * getReturn('ETHUSDT', 100) + 0.9956 * getReturn('ETHUSDT',
                                                                              500) - 0.4885 * getReturn(
                'ETHUSDT', 1000))
    ewmaManager.updateSymbolAll('SIGNAL', signal)
    signalEwma = signal - ewmaManager.getValue('SIGNAL',100)
    upperlimit = params['posUpperLimit']
    lowerlimit = params['posLowerLimit']
    signalProd = signalEwma if (signalEwma * signal) > 0 else 0
    #if signal and signalEwma have different sign, invalidate this signal
    midpos = 0.5 * (upperlimit + lowerlimit)

    mycurrentpos = pm.getPosition('BNB')

    mybid = bid - params['spread'] \
            - vol \
            + params['alphaMultiplier'] * signalProd \
            - params['positionSkew'] * (mycurrentpos - midpos)\
            + params['buysellSkew']

    myask = ask + params['spread'] \
            + vol \
            + params['alphaMultiplier'] * signalProd \
            - params['positionSkew'] * (mycurrentpos - midpos)\
            + params['buysellSkew']
    msg = 'vol,{:.4f}, signal,{:.4f}, signal-ewma,{:.4f}, myask,{:.4f}, mybid,{:.4f}, mid,{:.4f}'.format(vol, signal, signalEwma, myask, mybid, smid)
    print(msg)
    logging.debug(msg)
    return mybid, myask

LOG_FILENAME = 'example.log'
logging.basicConfig(
    filename=LOG_FILENAME,
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')
iloop = 0
print("start")
try:
    while True:
        iloop += 1
            # Thanks @seym45 for a fix
        try:

            marketdata = client.recv_string()
            _, data = marketdata.split(connTopic)
            res = json.loads(data)
        except zmq.ZMQError as error:
            print(error)
            continue

        if 'T' in res.keys():
            updateTime = datetime.fromtimestamp(res['T'] / 1000)
            if iloop % 100 == 0:
                latency =  datetime.now() - updateTime
                logging.debug(msg="market data latency:"+str(latency) )# prints periodly

        #  print(res.keys())
        if 'p' in res.keys():
 #           print("trade",res['s'])
            lastTradeManager.update(res['s'], float(res['p']))
        else:
            try:
                mybid, myask = updateBidAsk(res)

                if pm.getPosition('BNB') < params['posUpperLimit'] and params['ready'] == 1:
                    if abs(mybid - lubp) > 0.01:
                        om.cancelOrder(Action.BUY, 'BNBUSDT')
                        logging.debug('Buy@{:.2f}'.format(mybid))
                        tc.order_limit_buy(symbol='BNBUSDT', price=round(mybid, 4), quantity=1.0, recvWindow=10000)
                        lubp = mybid
                else:
                    om.cancelOrder(Action.BUY, 'BNBUSDT')

                if pm.getPosition('BNB') > params['posLowerLimit'] and params['ready'] == 1:
                    if abs(luap - myask) > 0.01 :
                        om.cancelOrder(Action.SELL, 'BNBUSDT')
                        logging.debug('Sell@{:.2f}'.format(myask))
                        tc.order_limit_sell(symbol='BNBUSDT', price=round(myask, 4), quantity=1.0, recvWindow=10000)
                        luap = myask
                else:
                    om.cancelOrder(Action.SELL, 'BNBUSDT')
            except Exception as e:
                print(e)
except Exception as e:
    print(e)

finally:
    tc.cancel_all_orders(symbol='BNBUSDT')

# print(type(res))

# print(int(datetime.now(tz=timezone.utc).timestamp() * 1000))
