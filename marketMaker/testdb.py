import pymongo
import zmq
import json
import time
dbClient = pymongo.MongoClient("python-binance.asuscomm.com:54321", username="kbai", password="9nurEfE1Ssy")
binanceDB = dbClient["binance"]
tradeCollection = binanceDB["AllBNBUSDT"]

import pandas as pd
import plotly.graph_objects as go


for symbol in [ 'ETHUSDT', 'BNBUSDT', 'BTCUSDT', 'LTCUSDT']:
    myquery = { "s" : symbol}
    t0 = time.time()
    df = list(tradeCollection.find({'s':'ETUHSDT'}))
    t1= time.time()
    print('load ', symbol, t1-t0)

