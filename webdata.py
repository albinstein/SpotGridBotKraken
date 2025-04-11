from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
from pymongo import MongoClient
import pandas as pd
import numpy as np
import threading
import time
import os
import json
from utils import fix_floats


api_key = 'YOUR API KEY'
api_secret = 'YOUR API KEY SECRET'



def update_mid_price(symbol, new_ask = None, new_bid = None):
    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor = db['SYMBOL_INFO']
    query = {'id':symbol}

    if not np.isnan(new_ask):
        update = {'$set':{'ask':new_ask}}
        cursor.update_one(query, update)

    if not np.isnan(new_bid):
        update = {'$set':{'bid':new_bid}}
        cursor.update_one(query, update)

    symbol_info = pd.DataFrame(list(cursor.find(query)))

    ask = symbol_info['ask'][0]
    bid = symbol_info['bid'][0]
    midprice = (ask+bid)/2

    update = {'$set':{'midprice':midprice}}
    cursor.update_one(query,update)
    db_client.close()


def print_stream(ws):

    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor = db['LOGGER']

    while True:
        if ws.is_manager_stopping():
            exit(0)
        new_data = ws.pop_stream_data_from_stream_buffer()
        if new_data is False:
            time.sleep(0.01)
        else:
            new_data = json.loads(new_data)
            if 'stream' in new_data:
                symbol = new_data['stream'].replace('@depth5','').upper()
                ask = float(new_data['data']['asks'][0][0])
                bid = float(new_data['data']['bids'][0][0])
                update_mid_price(symbol, ask, bid)

            elif 'executionReport' in new_data.values():
                if new_data['X'] != 'PARTIALLY_FILLED':
                    new_data['type'] = 'order'
                    new_data = fix_floats(new_data)
                    cursor.insert_one(new_data)
            elif 'outboundAccountPosition' in new_data.values():
                new_data['type'] = 'position'
                new_data = fix_floats(new_data)
                cursor.insert_one(new_data)


def run_websocket(symbols, channels):
    ws = BinanceWebSocketApiManager(exchange="binance.com")
    ws.create_stream(["arr"], ["!userData"], api_key=api_key, api_secret=api_secret, stream_label = "UserData")
    ws.create_stream(channels, symbols)
    worker_thread = threading.Thread(target=print_stream, args=(ws,))
    worker_thread.start()

    while True:
        os.system("clear")
        ws.print_summary()
        time.sleep(5)



# symbols = ['busdusdt']
# channels = ['depth5']

# run_websocket(symbols = symbols, channels = channels)
