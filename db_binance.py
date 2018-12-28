import pymongo
import datetime
from pymongo import UpdateOne

db_name = "binance_test"
m_client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = m_client[db_name]
CANDLE_KEYS = [
    'open_time',
    'open', 
    'high', 
    'low',
    'close',
    'volume', 
    'close_time', 
    'quote_asset_volume', 
    'number_of_trades', 
    'taker_buy_base_asset_volume', 
    'taker_buy_quote_asset_volume']

def addMultipleCandleData(data, crypto, tf, check = True):
    collection_str = crypto + 'USD_' + tf
    oldest_time = data[len(data)-1][0]
    try:
        if check == False:      # use insert
            res = [  dict(zip(CANDLE_KEYS,ts)) for ts in data]
            db[collection_str].insert_many(res)
        else :
            for ts in data:
                d = dict(zip(CANDLE_KEYS,ts))
                db[collection_str].update_one({'t': ts[0]}, {'$set': d}, upsert= True) # insert


    except Exception as e:
        print("error!!!!!!!!!!!!!! ::")
        print(e)
        print("data ::")
        print(data)
        
    return oldest_time

def getOldestTS(crypto, tf):
    collection_str = crypto + 'USD_' + tf
    if db[collection_str].find({}).sort([("open_time", +1)]).count() >= 1:
        return db[collection_str].find({}).sort([("open_time", +1)]).limit(1)[0]['open_time']
    else:
        return int(datetime.datetime.now().timestamp())* 1000

def getLatestTS(crypto, tf):
    collection_str = crypto + 'USD_' + tf
    if db[collection_str].find({}).sort([("open_time", -1)]).count() >= 1:
        return db[collection_str].find({}).sort([("open_time", -1)]).limit(1)[0]['open_time']
    else:
        return int(datetime.datetime.now().timestamp())* 1000
