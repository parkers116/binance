import requests
import db_binance
import datetime
import queue, threading, sys, time

num_thread = 4  # base on cores
targeting_crypto = ['BTC','ETH','LTC','NEO','XRP']
targeting_tf = ['1m','5m']
oldest_record = dict()
latest_record = dict()
thread_q = queue.Queue()
END_POINT = 'https://api.binance.com/'

task = 'update'

def price_parser(price):
    try:
        result_list = []
        for a in price.split(','):
            result_list.append(float(a))
        price_object = {
            'open_time':int(result_list[0]),
            'open':result_list[1],
            'high':result_list[2],
            'low':result_list[3],
            'close':result_list[4],
            'volume':result_list[5],
            'close_time':int(result_list[6]),
            'quote_asset_volume':result_list[7],
            'number_of_trades':result_list[8],
            'taker_buy_base_asset_volume':result_list[9],
            'taker_buy_quote_asset_volume':result_list[10]

        }
        return price_object
    except Exception as e:
        print('exception in price parser: ',e)
        #send_error_email('price_parser(price)',str(ex),str(traceback.format_exc()))
        return ''

def parse_ts_ms (do):
  return int(do.timestamp()) * 1000

def get_candle_url(freq, symbol, limit, start, end):
  #url =  END_POINT + "v2/candles/trade:{0}:{1}/hist".format(freq,'t'+symbol+'USD')
  url =  END_POINT + "api/v1/klines?symbol={0}USDT&interval={1}&startTime={2}&endTime={3}&limit={4}".format(symbol, freq, start, end, limit)
  return requests.get(url)

def batch_update(wid):
    while not thread_q.empty():
        tf ,crypto = thread_q.get()
        d_time = datetime.timedelta(minutes=5) if tf == '5m' else datetime.timedelta(minutes=1)
        end_time = datetime.datetime.now()
        start_time = datetime.datetime.fromtimestamp( int ( int(latest_record[crypto+'USD_'+tf]) /1000 ) + 1 ) 
        limit = 1000

        if end_time - d_time <= start_time:
            print('end getting data of {0} {1} on {2} to {3} '.format(tf,crypto,start_time, end_time))
            thread_q.task_done()
            break
        
        try:
            db = db_binance.db
            coll = db[crypto+'USD_'+tf]
            price_object_list = []
            response = get_candle_url(tf,crypto,limit,parse_ts_ms(start_time),parse_ts_ms(end_time)).text
            for price in response[1:-1].split('],')[-limit-1:-1]:
                try:
                    price_object = price_parser(price.replace('[','').replace(']','').replace('"',''))
                    if price_object == '':
                        continue
                    #print(price_object['open_time'],crypto+'USDT',tf)
                    price_object_list.append(price_object)
                    #coll.update({'open_time':price_object['open_time']},price_object,upsert=True)
                except Exception as e:
                    print('exception in for loop: ',e)
            try:
                coll.insert_many(price_object_list)
                #print('finish inserting', crypto, 'USD_', tf, ' dataset')
                print('end getting data of {0} {1} on {2} to {3} '.format(tf,crypto,start_time, end_time))
                thread_q.task_done()
            except Exception as e:
                print('exception in inserting data to database: ',e)
            
        except Exception as e:
            print('exception in main: ',e)

def crawler(crypto, tf):
    try:
        db = db_binance.db
        coll = db[crypto+'USD_'+tf]
        limit = 1000
        response = get_candle_url(tf,crypto,limit,1537401600000,1537488000000).text
        price_object_list = []
        for price in response[1:-1].split('],')[-limit-1:-1]:
            try:
                price_object = price_parser(price.replace('[','').replace(']','').replace('"',''))
                if price_object == '':
                    continue
                #print(price_object['open_time'],crypto+'USDT',tf)
                price_object_list.append(price_object)
                #coll.update({'open_time':price_object['open_time']},price_object,upsert=True)
            except Exception as e:
                print('exception in for loop: ',e)
        try:
            coll.insert_many(price_object_list)
            print('finish inserting', crypto, 'USD_', tf, ' dataset')
        except Exception as e:
            print('exception in inserting data to database: ',e)
    except Exception as e:
        print('exception in main: ',e)

def init():
  for crypto in targeting_crypto: # ['XRP','BTCSV','BTCABC' ,'ETH','EOS','BCH','LTC','ETC','NEO','IOT','MGO']
    for tf in targeting_tf: # ['1m','5m']
      # condition
      oldest_record[crypto+'USD_'+ tf] = db_binance.getOldestTS(crypto,tf)
      latest_record[crypto+'USD_'+ tf] = db_binance.getLatestTS(crypto,tf)

      thread_q.put((tf,crypto))
      print('latest record of {}_ {} is : {}'.format(crypto,tf,latest_record[crypto+'USD_'+ tf]))

def main():
  for i in range(num_thread):
    if task == 'update':
      t = threading.Thread(target=batch_update,args=(i,))
      t.start()

"""
if task =='update':
  while True:
    print('update record !')
    init()
    main()
    print('end update')
    #time.sleep(10)
"""

#the code on below is onyly for testing 

"""
#crawler('BTC','5m')
for i in targeting_crypto:
    for j in targeting_tf:
        crawler(i,j)
print('complete crawling hist data from 1545177600000 to 1545264000000')
"""

"""
init()
"""

"""
print(datetime.datetime.fromtimestamp(1546026900000/1000))
"""