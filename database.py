import pandas as pd
from pymongo import MongoClient


def update_balances():
    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor = db['USER_INFO']
    cursor_balances = db['BALANCES']

    df = pd.DataFrame(list(cursor.find()))

    if len(df) > 0:
        df.sort_values('E', ascending = True, inplace = True)
        updates = df['B'].iloc[-1]

        for update in updates:

            update['asset'] = update.pop('a')
            update['free'] = update.pop('f')
            update['locked'] = update.pop('l')

            query = {'asset':update['asset']}

            cursor_balances.update_one(query, {"$set":update})
        idx = df['_id']

        for id in idx:
            query = {'_id':id}
            cursor.delete_one(query)
    db_client.close()

def update_orders():
    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor_active_orders = db['ACTIVE_ORDERS']
    cursor_historical_orders = db['HISTORICAL_ORDERS']

    df = pd.DataFrame(list(cursor_active_orders.find()))
    if len(df) > 0:
        new, filled, cancelled = None, None, None

        if 'NEW' in df.values:
            new = df[df['X'] == 'NEW']
            idx = new['i']

            amended = []
            for id in idx:
                orders = new[new['i'] == id]
                if (len(orders) > 1) and (id not in amended):
                    orders = orders.sort_values(by = 'T', ascending = False)
                    orders.drop(orders.index[0], inplace = True)
                    for i in range(len(orders)):
                        query = {"_id":orders["_id"].iloc[i]}
                        cursor_active_orders.delete_many(query)
                    amended.append(id)
            del amended, idx, new

        if 'FILLED' in df.values:
            filled = df[df['X'] == 'FILLED']
            cursor_historical_orders.insert_many(filled.to_dict('records'))
            idx = filled['i']
            for id in idx:
                query = {'i':id}
                cursor_active_orders.delete_many(query)
            del idx, filled

        if 'CANCELED' in df.values:
            cancelled = df[df['X'] == 'CANCELED']
            idx = cancelled['i']
            for id in idx:
                query = {'i':id}
                cursor_active_orders.delete_many(query)
            del idx, cancelled
    db_client.close()

def update_database():
    print('Updating Database')
    db_client = MongoClient('localhost')
    db = db_client['BINANCE']
    cursor = db['LOGGER']

    types = ['order', 'position']
    collections = ['ACTIVE_ORDERS', 'USER_INFO']
    while True:
        for type, collection in zip(types, collections):
            sub_cursor = db[collection]
            query = {"type":type}
            df = pd.DataFrame(list(cursor.find(query)))
            if len(df) > 0:
                idx = df['_id']
                df.drop(columns = ['_id'], inplace = True)
                temp_dict = df.to_dict('records')
                sub_cursor.insert_many(temp_dict)

                for id in idx:
                    query = {"_id":id}
                    cursor.delete_one(query)
            update_orders()
            update_balances()
