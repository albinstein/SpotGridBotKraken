import sys
sys.path.append("../")

import pandas as pd
import ccxt

from multiprocessing import Process
import time

from database import update_database
from webdata import run_websocket
from orders import update_orders
from gridbot import GridBot

class BotController:
    def __init__(self, symbols, strategy):
        self.symbols = symbols
        self.strategy = strategy
       
        self.exchange = ccxt.binance({
                'apiKey': 'YOUR API KEY',
                'secret': 'YOUR API KEY SECRET'],
                'enableRateLimit':True,
                'timeout':3000,
                'options': {
                    'defaultType': 'spot',
                }
                })

    def bot_runner(self):
        websymbols = [(symbol.replace('/', '').lower()) for symbol in self.symbols]
        p1 = Process(target = run_websocket, args = (websymbols, ['depth5']))
        p1.start()
        p2 = Process(target = update_database)
        p2.start()
        time.sleep(3)

        while True:
            try:
                for symbol in self.symbols:
                    new_orders, cancel_orders = self.strategy.get_orders(symbol = symbol)
                    print(symbol)
                    update_orders(exchange = self.exchange, new_orders = new_orders, cancel_orders = cancel_orders)
                    del new_orders, cancel_orders
            except Exception as e:
                print(e)

symbols = ['LTC/BUSD','ETH/BUSD','ADA/BUSD','LINK/BUSD']

params = {
'n_grids': 5,
'p_grids': 3,
's_grids': 1.05,
}

strategy = GridBot(params = params)
model = BotController(symbols = symbols, strategy = strategy)
model.bot_runner()
