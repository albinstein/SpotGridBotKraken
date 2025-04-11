import ccxt
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
import time

def format_price_and_amount(symbol, price, amount):
    '''
    This functions gives format to price and amount,
    in order of filter precision.
    '''
    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor = db['SYMBOL_INFO']
    query = {"symbol":symbol}
    symbol_info = cursor.find_one(query)
    amount = round(amount, int(symbol_info['precision']['amount']))
    price = round(price, int(symbol_info['precision']['price']))
    db_client.close()
    return price, amount

def update_orders(exchange, new_orders = [], cancel_orders = []):
    executor = ThreadPoolExecutor(max_workers = 5)
    for order in new_orders:
        result = executor.submit(create_order, exchange, order)
        # print(result.result())

    for order in cancel_orders:
        result = executor.submit(cancel_order, exchange, order)
        # print(result.result())

    time.sleep(1)

def check_balances(order = {}):
    flag = False
    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor = db['BALANCES']
    assets = order['symbol'].split('/')
    if order['side'] == 'buy':
        amount = order['amount'] * order['price']
        query = {'asset':assets[1]}
        balance = cursor.find_one(query)

        if balance['free'] > amount:
            flag = True

    elif order['side'] == 'sell':
        query = {'asset':assets[0]}
        balance = cursor.find_one(query)
        if balance['free'] > order['amount']:
            flag = True
    db_client.close()
    return flag

def create_order(exchange, params_order = {}):

    price, amount = format_price_and_amount(symbol = params_order['symbol'], price = params_order['price'], amount = params_order['amount'])
    params_order['price'] = price
    params_order['amount'] = amount
    if check_balances(order = params_order):
        order = exchange.create_order(
        symbol = params_order['symbol'],
        type = params_order['type'], # 'market', 'limit'
        side = params_order['side'], # 'buy', 'sell'
        price = price,
        amount = amount,
        params = params_order['params']
        )


def cancel_order(exchange, params_order = {}):
    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor = db['SYMBOL_INFO']
    query = {"id":params_order['s']}
    symbol_info = cursor.find_one(query)

    exchange.cancel_order(symbol = symbol_info['symbol'], id = int(params_order['i']))
    db_client.close()
