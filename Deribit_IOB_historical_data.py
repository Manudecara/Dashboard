#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 28 10:33:41 2021

@author: manu
"""


import asyncio
import websockets
import json
import pandas as pd
import numpy as np
from scipy.stats import norm
pd.options.mode.chained_assignment = None
import nest_asyncio
import glom
import math
import datetime as dt
from datetime import timezone
import calendar
from time import sleep
from operator import itemgetter
nest_asyncio.apply()

#add the client id and client secret of the account to run this code on.

currency = 'BTC'

    
class DeribitWS:

    def __init__(self, client_id, client_secret, live=False):

        if not live:
            self.url = 'wss://test.deribit.com/ws/api/v2'
        elif live:
            self.url = 'wss://www.deribit.com/ws/api/v2'
        else:
            raise Exception('live must be a bool, True=real, False=paper')
            
        
        self.client_id = client_id
        self.client_secret = client_secret
        
        self.auth_creds = {
              "jsonrpc" : "2.0",
              "id" : 0,
              "method" : "public/auth",
              "params" : {
                "grant_type" : "client_credentials",
                "client_id" : self.client_id,
                "client_secret" : self.client_secret
              }
            }
        self.test_creds()
        
        self.msg = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": None,
        }

    async def pub_api(self, msg):
        async with websockets.connect(self.url) as websocket:
            await websocket.send(msg)
            while websocket.open:
                response = await websocket.recv()
                return json.loads(response)

    async def priv_api(self, msg):
        async with websockets.connect(self.url) as websocket:
            await websocket.send(json.dumps(self.auth_creds))
            while websocket.open:
                response = await websocket.recv()
                await websocket.send(msg)
                response = await websocket.recv()
                break
            return json.loads(response)

    @staticmethod
    def async_loop(api, message):
        return asyncio.get_event_loop().run_until_complete(api(message))


    def test_creds(self):
        response = self.async_loop(self.pub_api, json.dumps(self.auth_creds))
        if 'error' in response.keys():
            raise Exception(f"Auth failed with error {response['error']}")
        else:
            print("Auth creds are good, it worked")
            
            
                          ### TRADINGS METHODS ###


    def market_order(self, instrument, amount, direction):
        params = {
                "instrument_name" : instrument,
                "amount" : amount,
                "type" : "market",
              }

        if direction.lower() == 'long':
            side = 'buy'
        elif direction.lower() == 'short':
            side = 'sell'
        else:
            raise ValueError('direction must be long or short')

        self.msg["method"] = f"private/{side}"
        self.msg["params"] = params
        response = self.async_loop(self.priv_api, json.dumps(self.msg))
        return response


    def limit_order(self, instrument, amount, direction, price,
                   post_only, reduce_only):
        params = {
            "instrument_name": instrument,
            "amount": amount,
            "type": "limit",
            "price": price,
            "post_only":  post_only,
            "reduce_only": reduce_only

        }
        if direction.lower() == 'long':
            side = 'buy'
        elif direction.lower() == 'short':
            side = 'sell'
        else:
            raise ValueError('direction must be long or short')

        self.msg["method"] = f"private/{side}"
        self.msg["params"] = params
        response = self.async_loop(self.priv_api, json.dumps(self.msg))
        return response
    
    def cancel_order(self, order_id):
        params = {
                "order_id" : order_id
              }

        self.msg["method"] = "private/cancel"
        self.msg["params"] = params
        response = self.async_loop(self.priv_api, json.dumps(self.msg))
        return response
    
    def edit_order(self, order_id, amount, price):
        params = {
                "order_id" : order_id,
                "amount" : amount,
                "price" : price
              }
        self.msg["method"] = "private/edit"
        self.msg["params"] = params
        response = self.async_loop(self.priv_api, json.dumps(self.msg))
        return response

                          ### MARKET DATA METHODS ###
                          
                          
    def get_data(self, instrument, start, end, timeframe):
        params =  {
                "instrument_name": instrument,
                "start_timestamp": start,
                "end_timestamp": end,
                "resolution": timeframe
            }

        self.msg["method"] = "public/get_tradingview_chart_data"
        self.msg["params"] = params

        data = self.async_loop(self.pub_api, json.dumps(self.msg))
        return data

    def get_orderbook(self, instrument, depth=10):
        params = {
            "instrument_name": instrument,
            "depth": depth
        }
        self.msg["method"] = "public/get_order_book"
        self.msg["params"] = params

        order_book = self.async_loop(self.pub_api, json.dumps(self.msg))
        return order_book
    
    def get_instruments(self, currency, kind, expired):
        params = {
            "currency": currency,
            "kind": kind,
            "expired": expired
        }
        self.msg["method"] = "public/get_instruments"
        self.msg["params"] = params
        instruments = self.async_loop(self.pub_api, json.dumps(self.msg))
        return instruments
    
    def get_last_trades_by_instrument(self, instrument_name, count, include_old, sorting):
        params = {
            "instrument_name": instrument_name,
            "count": count,
            "include_old": include_old,
            "sorting": sorting
        }
        self.msg["method"] = "public/get_last_trades_by_instrument"
        self.msg["params"] = params
        last_trades = self.async_loop(self.pub_api, json.dumps(self.msg))
        return last_trades['result']['trades']
    
    def get_last_trades_by_instrument_and_time(self, instrument_name, start_timestamp, end_timestamp, count, sorting):
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "count": count,
            "sorting": sorting
        }
        self.msg["method"] = "public/get_last_trades_by_instrument_and_time"
        self.msg["params"] = params
        last_trades_by_time = self.async_loop(self.pub_api, json.dumps(self.msg))
        return last_trades_by_time['result']['trades'] 
    
    def get_last_trades_by_currency_and_time(self, currency, kind, start_timestamp, end_timestamp, count, include_old, sorting):
        params = {
            "currency": currency,
            "kind": kind,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "count": count,
            "include_old": include_old,
            "sorting": sorting
        }
        self.msg["method"] = "public/get_last_trades_by_currency_and_time"
        self.msg["params"] = params
        last_trades_by_currency_and_time = self.async_loop(self.pub_api, json.dumps(self.msg))
        return last_trades_by_currency_and_time['result']['trades'] 
    
    def get_trade_volumes(self, extended):
        params = {
            "extended": extended,
        }
        self.msg["method"] = "public/get_trade_volumes"
        self.msg["params"] = params
        last_volumes = self.async_loop(self.pub_api, json.dumps(self.msg))
        return last_volumes
    
    def get_open_orders_by_currency(self, currency, kind):
        params = {
            "currency": currency,
            "kind": kind
        }
        self.msg["method"] = "private/get_open_orders_by_currency"
        self.msg["params"] = params
        open_orders = self.async_loop(self.priv_api, json.dumps(self.msg))
        return open_orders
    
    def get_open_orders_by_instrument(self, instrument_name):
        params = {
            "instrument_name": instrument_name
        }
        self.msg["method"] = "private/get_open_orders_by_instrument"
        self.msg["params"] = params
        open_orders = self.async_loop(self.priv_api, json.dumps(self.msg))
        return open_orders

    def get_quote(self, instrument):
        params = {
            "instrument_name": instrument
        }
        self.msg["method"] = "public/ticker"
        self.msg["params"] = params
        quote = self.async_loop(self.pub_api, json.dumps(self.msg))
        return quote['result']['last_price']
    
    def get_index(self, currency):
        params = {
            'currency':currency
        }
        self.msg["method"] = "public/get_index"
        self.msg["params"] = params
        price = self.async_loop(self.pub_api, json.dumps(self.msg))
        return price
    
    def get_order_state(self, order_id):
        params = {
            'order_id': order_id
        }
        self.msg["method"] = "private/get_order_state"
        self.msg["params"] = params
        state = self.async_loop(self.priv_api, json.dumps(self.msg))
        return state

                          ### ACCOUNT DATA METHODS ###
                          

    def get_announcements(self, count):
        params = {
            "count": count
        }
        self.msg["method"] = "public/get_announcements"
        self.msg["params"] = params
        announcements = self.async_loop(self.pub_api, json.dumps(self.msg))
        return announcements
    
    def account_summary(self, currency, extended=True):
        params = {
            "currency": currency,
            "extended": extended
        }
        self.msg["method"] = "private/get_account_summary"
        self.msg["params"] = params
        summary = self.async_loop(self.priv_api, json.dumps(self.msg))
        return summary

    def get_positions(self, currency, kind):
        params = {
            "currency": currency,
            "kind": kind
        }
        self.msg["method"] = "private/get_positions"
        self.msg["params"] = params
        positions = self.async_loop(self.priv_api, json.dumps(self.msg))
        return positions
    
    def get_position(self, instrument_name):
        params = {
            "instrument_name": instrument_name
        }
        self.msg["method"] = "private/get_position"
        self.msg["params"] = params
        position = self.async_loop(self.priv_api, json.dumps(self.msg))
        return position
    
        

if __name__ == '__main__':
    with open('/Users/manu/Quant Finance/OakTree & Lion/auth_creds_mm.json') as j:
        creds = json.load(j)

    client_id = creds['real']['client_id']
    client_secret = creds['real']['client_secret']

    ws = DeribitWS(client_id=client_id, client_secret=client_secret, live=True)
    
    def get_options():
    
        options = ws.get_instruments(currency, kind='option', expired = False)
        options = pd.DataFrame(options['result'])
        expired_options = ws.get_instruments(currency, kind='option', expired = True)
        expired_options = pd.DataFrame(expired_options['result'])
        options = options.append(expired_options)
        options['creation_timestamp'] = pd.to_datetime(options['creation_timestamp'], unit= 'ms')
        options['expiration_timestamp'] = pd.to_datetime(options['expiration_timestamp'], unit= 'ms')
        options['TTM'] = pd.to_datetime(options['expiration_timestamp']) - pd.to_datetime(options['creation_timestamp'])
        
        return options
    
    options = get_options() 
    
    
    all_days = []
    date_all_days = []
    
    present = pd.to_datetime(str(dt.datetime.utcnow())[:-15] + '08:00:00')
    date_all_days.append(present)
    all_days.append(int(str(calendar.timegm(present.timetuple())) + '000'))

    for i in list(range(1,4)): #adjust for number of days
        past = present - dt.timedelta(days=i)
        date_all_days.append(past)
        all_days.append(int(str(calendar.timegm(past.timetuple())) + '000'))
    
    end = list(range(0,len(all_days)))
    start = list(range(1,len(all_days)))
    end = end[:-1]
    end = list(reversed(end))
    start = list(reversed(start))
    
    
    #get all trades available for every market
    all_markets = []
    for i in options.instrument_name:
        all_markets.append(ws.get_last_trades_by_instrument(instrument_name = i, 
                                                            count = 1000,
                                                            include_old = True,
                                                            sorting = 'desc'))    
    
    #update trades by day
    markets = []
    for s,e in zip(start, end):
        for i in options.instrument_name:
            markets.append(ws.get_last_trades_by_instrument_and_time(instrument_name = i, 
                                                                     start_timestamp = all_days[s], 
                                                                     end_timestamp = all_days[e], 
                                                                     count = 1000,
                                                                     sorting = 'desc'))    
            
    #update with last trades made 
    last_trades = []
    for s,e in zip(start, end):
        last_trades.append(ws.get_last_trades_by_currency_and_time(currency= 'BTC', 
                                                               kind= 'option', 
                                                               start_timestamp= all_days[s], 
                                                               end_timestamp= all_days[e], 
                                                               count= 1000, 
                                                               include_old= False, 
                                                               sorting= 'desc'))
        
    
    
    def DDOI():
    
        ddoi = []
        for market in all_markets:
            for trade in market:
                ddoi.append(trade.get('instrument_name').rpartition('-')[2])                                        #C or P
                ddoi.append(trade.get('direction'))                                                                 #d = Buy or Sell
                ddoi.append(trade.get('index_price'))                                                               #s = underlying                                                             
                ddoi.append(int(trade.get('instrument_name').rpartition('-')[0].rpartition('-')[2]))                #k = strike
                expiry = trade.get('instrument_name').rpartition('-')[0].rpartition('-')[0].rpartition('-')[2]
                months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
                for m in months:
                    if m in expiry:
                        month = m
                m = (' ' + str(months.index(month) + 1) + ' ')
                expiry = expiry.replace(month, m)
                expiry = dt.datetime.strptime(expiry, '%d %m %y')
                creation = pd.to_datetime(trade.get('timestamp'), unit= 'ms')
                ddoi.append((expiry - creation).days/365)                                                           #t = time to expiry
                ddoi.append(0)                                                                                      #r = risk-free rate
                ddoi.append(trade.get('iv'))                                                                        #v = volatility
                
        return ddoi
    
    ddoi = DDOI()
    
    def delta(flag, s, k, t, r, v):
        d1 = (np.log(s/k)+(r+v*v/2)*t)/(v*np.sqrt(t))
        if flag == "C":
            return norm.cdf(d1)
        else:
            return norm.cdf(-d1) # +signed put delta 
    
               
    def GEX():            
        
        flag = list(range(0, len(ddoi), 7))  
        direction = list(range(1, len(ddoi), 7)) 
        underlying = list(range(2, len(ddoi), 7))    
        strike = list(range(3, len(ddoi), 7)) 
        ttm = list(range(4, len(ddoi), 7))    
        rfr = list(range(5, len(ddoi), 7))    
        vol = list(range(6, len(ddoi), 7))
        
        deltas = []
        ivs = []
        net_put_deltas = []
        net_call_deltas = []
        puts_sold = []
        puts_bought = []
        calls_sold = []
        calls_bought = []
        
        for f,d,u,s,t,r,v in zip(flag, direction, underlying, strike, ttm, rfr, vol):
            deltas.append(delta(ddoi[f],  ddoi[d], ddoi[u],  ddoi[s],  ddoi[t],  ddoi[r],  ddoi[v]))
            ivs.append(ddoi[v]/100)
        
        for f,d,u,s,t,r,v in zip(flag, direction, underlying, strike, ttm, rfr, vol):
            if ddoi[f] == 'P':
                net_put_deltas.append(delta(ddoi[f],  ddoi[d], ddoi[u],  ddoi[s],  ddoi[t],  ddoi[r],  ddoi[v]))
                if ddoi[d] == 'buy':
                    puts_bought.append(delta(ddoi[f],  ddoi[d], ddoi[u],  ddoi[s],  ddoi[t],  ddoi[r],  ddoi[v]))
                else:
                    puts_sold.append(delta(ddoi[f],  ddoi[d], ddoi[u],  ddoi[s],  ddoi[t],  ddoi[r],  ddoi[v]))

            elif ddoi[f] == 'C':
                net_call_deltas.append(delta(ddoi[f],  ddoi[d], ddoi[u],  ddoi[s],  ddoi[t],  ddoi[r],  ddoi[v]))
                if ddoi[d] == 'buy':
                    calls_bought.append(delta(ddoi[f],  ddoi[d], ddoi[u],  ddoi[s],  ddoi[t],  ddoi[r],  ddoi[v]))
                else:
                    calls_sold.append(delta(ddoi[f],  ddoi[d], ddoi[u],  ddoi[s],  ddoi[t],  ddoi[r],  ddoi[v]))

        
        deltas = [i for i in deltas if math.isnan(i) == False]
        net_put_deltas = [i for i in net_put_deltas if math.isnan(i) == False]
        net_call_deltas = [i for i in net_call_deltas if math.isnan(i) == False]

        gex = sum(deltas)
        vex = sum(ivs)
        gexPLUS = gex + vex
        
        npd = sum(net_put_deltas)
        ncd = sum(net_call_deltas)
        
        
        return gex,vex,gexPLUS,npd,ncd
    
    values = GEX()
    

