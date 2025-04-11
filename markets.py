import ccxt
import pandas as pd
from pymongo import MongoClient
from pprint import pprint
from utils import fix_floats

def update_markets(exchange):
    client = MongoClient('localhost')
    db = client['BINANCE']
    cursor = db['SYMBOL_INFO']
    cursor_balances = db['BALANCES']
    balances = exchange.fetch_balance()['info']['balances']

    for balance in balances:
        balance = fix_floats(balance)
        query = {"asset":balance['asset']}
        update = {"$set":balance}
        cursor_balances.update_many(query, update, True)

    markets = exchange.fetch_markets()

    in_db = pd.DataFrame(list(cursor.find()))
    temp = []

    if len(in_db) != 0:
        temp = list(in_db['symbol'])

    for market in markets:

        symbol = market['symbol']

        symbol_info = {
        'symbol': symbol,
        'id':market['id'],
        'precision':market['precision'],
        'minNotional':market['info']['filters'][3]['minNotional'],
        'ask':0,
        'bid':0,
        'midprice':0,
        }

        if len(in_db) == 0:
            cursor.insert_one(symbol_info)

        else:

            if symbol in temp:
                query = {'symbol': symbol}
                update = {'$set': symbol_info}
                cursor.update_one(query, update)
                temp.remove(symbol)
            else:
                cursor.insert_one(symbol_info)

    if len(temp) != 0:
        for symbol in temp:
            query = {'symbol':symbol}
            cursor.delete_one(query)
    client.close()


keys = pd.read_csv("../UsuariosDomingo.csv", header = 0)
exchange = ccxt.binance({
        'apiKey': 'YOUR API KEY',
        'secret': 'YOUR API KEY SECRET',
        'enableRateLimit':True,
        'timeout':3000,
        'options': {
            'defaultType': 'spot',
        }
        })
update_markets(exchange)
