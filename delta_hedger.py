#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 14 10:37:07 2021

@author: manu
"""


import asyncio
import websockets
import json
import pandas as pd
import nest_asyncio
import glom
import datetime
from datetime import timezone
import calendar
from datetime import datetime
from time import sleep
nest_asyncio.apply()


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
    
    def get_instruments(self, currency, kind, expired=False):
        params = {
            "currency": currency,
            "kind": kind,
            "expired": expired
        }
        self.msg["method"] = "public/get_instruments"
        self.msg["params"] = params
        instruments = self.async_loop(self.pub_api, json.dumps(self.msg))
        return instruments
    
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
    with open('/Users/manu/Quant Finance/OakTree & Lion/auth_creds_delta.json') as j:
        creds = json.load(j)

    client_id = creds['paper']['client_id']
    client_secret = creds['paper']['client_secret']

    ws = DeribitWS(client_id=client_id, client_secret=client_secret, live=False)
    
    
    hedging_index = 2
    
    hist_prices = []
    
    while True:
        
        print('')
        
        if hist_prices == []:
            new_quote = ws.get_index('BTC')
            new_quote = new_quote['result']['BTC']
            old_quote = new_quote
            hist_prices.append(old_quote)
            print('BTC price now is: ' + str(old_quote))
            print('')
            sleep(60)
        
        elif hist_prices != []:
            new_quote = ws.get_index('BTC')
            new_quote = new_quote['result']['BTC']
            print('BTC last hedging price was: ' + str(hist_prices[-1]))
            print('BTC price now is: ' + str(new_quote))
            print('')

            
            def get_change(current, previous):
                if current == previous:
                    return 100.0
                try:
                    return (abs(current - previous) / previous) * 100.0
                except ZeroDivisionError:
                    return 0
            change = get_change(current = new_quote, previous = hist_prices[-1])
            
            if change >= hedging_index:
                old_quote = new_quote
                hist_prices.append(old_quote)
                print('There has been a move in the underlying higher than the hedging index of ' + str(hedging_index) + '%.\nDelta Hedger is running.')
                print('Change: ' + str(change) + '%')
                break
            
            elif change <= hedging_index:
                print('There hasnt been a significant move in the underlying higher than the hedging index of ' + str(hedging_index) + '%.\nDelta Hedger not needed.')
                print('Change: ' + str(change) + '%')
                sleep(60)
       

    
    print('')
    print('BEGINNING HEDGE')
    print('==============')
    
    
    
    def spot_hedger():
        #check to hedge portfolio delta
        summary = ws.account_summary(currency='BTC')
        summary = pd.DataFrame(summary['result'])
        equity = summary.equity[0] 
        
        price = ws.get_index('BTC')
        price = price['result']
        price = price['BTC']
        
        future_positions = ws.get_positions(currency='BTC', kind='future')
        future_positions = future_positions['result']
        future_positions[:] = [i for i in future_positions if i.get('direction') != 'zero']
        future_positions = pd.DataFrame(future_positions)
        
        perpetual = 'BTC-PERPETUAL'
        
        if future_positions.empty == True or perpetual not in list(future_positions.instrument_name):
            future_positions["instrument_name"] = []
            print('no perpetual positions')
            #for 1st time
            hedge_usd = equity * price
            spot_delta = hedge_usd
            
        elif future_positions.empty == False or perpetual in list(future_positions.instrument_name):
            future_positions = future_positions[future_positions.instrument_name.str.contains("BTC-PERPETUAL")]
            print('we have perpetual positions')
            #for stream
            spot_delta = float(equity + future_positions['delta'])
            hedge_usd = float(spot_delta * price)
            
        def round(n):
            a = (n // 10) * 10                   # Smaller multiple
            b = a + 10                           # Larger multiple
            return (b if n - a > b - n else a)   # Return of closest of two
        hedge_usd = round(hedge_usd)
        
        if spot_delta > 0.01 or spot_delta < -0.01:
            print('')
            
            #short perpetual
            if spot_delta > 0.01:
                #print('spot delta is: ' + str(spot_delta) + ' so we have to hedge')
                print('-------------------------------------------------------------------------------')
                m_order = ws.market_order('BTC-PERPETUAL', abs(hedge_usd), 'short') #long or short
                print('SELL order was sent for ' + str(abs(hedge_usd)) + ' USD on ' + 'BTC-PERPETUAL')
                m_order = pd.DataFrame(m_order)
                #check if order was filled
                if m_order.columns[2] == 'error':
                    m_order = list(m_order.error)   
                    reason = m_order[1]['reason']
                    print('order was not filled because it ' + reason)
                elif m_order.columns[2] == 'result':     
                    m_order = list(m_order.result) 
                    while True:
                        order_id = m_order[0]['order_id']
                        order_state = ws.get_order_state(order_id)
                        order_state = order_state['result']['order_state']
                        if order_state != 'open':
                            print('order was ' + order_state)
                            break
                        elif order_state == 'open':
                            print('order is ' + order_state + ' and waiting to get filled')
                            sleep(3)
                    
                #check new spot delta 
                summary = ws.account_summary(currency='BTC')
                summary = pd.DataFrame(summary['result'])
                equity = summary.equity[0] 
                
                future_positions = ws.get_positions(currency='BTC', kind='future')
                future_positions = future_positions['result']
                future_positions[:] = [i for i in future_positions if i.get('direction') != 'zero']
                future_positions = pd.DataFrame(future_positions)
                future_positions = future_positions[future_positions.instrument_name.str.contains("BTC-PERPETUAL")]
            
                perpetual = 'BTC-PERPETUAL'
                
                if future_positions.empty == True or perpetual not in list(future_positions.instrument_name):
                    print('spot was not hedged')
                    #for 1st time
                    spot_delta = equity * price
                    
                elif future_positions.empty == False or perpetual in list(future_positions.instrument_name):
                    print('spot was hedged')
                    #for stream
                    spot_delta = float(equity + future_positions['delta'])
                
                print('new spot delta is: ' + str(spot_delta))
                print('')
            
            #buy perpetual
            elif spot_delta < -0.01:
                print('spot delta is: ' + str(spot_delta) + ' so we have to hedge')
                print('-------------------------------------------------------------------------------')
                m_order = ws.market_order('BTC-PERPETUAL', abs(hedge_usd), 'long') #long or short
                print('BUY order was sent for ' + str(abs(hedge_usd)) + ' USD on ' + 'BTC-PERPETUAL')
                m_order = pd.DataFrame(m_order)
                #check if order was filled
                if m_order.columns[2] == 'error':
                    m_order = list(m_order.error)   
                    reason = m_order[1]['reason']
                    print('order was not filled because it ' + reason)
                elif m_order.columns[2] == 'result':     
                    m_order = list(m_order.result) 
                    while True:
                        order_id = m_order[0]['order_id']
                        order_state = ws.get_order_state(order_id)
                        order_state = order_state['result']['order_state']
                        if order_state != 'open':
                            print('order was ' + order_state)
                            break
                        elif order_state == 'open':
                            print('order is ' + order_state + ' and waiting to get filled')
                            sleep(3)
                    
                #check new spot delta
                summary = ws.account_summary(currency='BTC')
                summary = pd.DataFrame(summary['result'])
                equity = summary.equity[0] 
                
                future_positions = ws.get_positions(currency='BTC', kind='future')
                future_positions = future_positions['result']
                future_positions[:] = [i for i in future_positions if i.get('direction') != 'zero']
                future_positions = pd.DataFrame(future_positions)
                future_positions = future_positions[future_positions.instrument_name.str.contains("BTC-PERPETUAL")]
            
                perpetual = 'BTC-PERPETUAL'
                
                if future_positions.empty == True or perpetual not in list(future_positions.instrument_name):
                    print('spot was not hedged')
                    #for 1st time
                    spot_delta = equity * price
                    
                elif future_positions.empty == False or perpetual in list(future_positions.instrument_name):
                    print('spot was hedged')
                    #for stream
                    spot_delta = float(equity + future_positions['delta'])
                
                print('new spot delta is: ' + str(spot_delta))
                print('')
                    
        elif spot_delta < 0.01 and spot_delta > -0.01:
            print('spot is hedged with spot delta: ' + str(spot_delta))
            print('')
        return
    
    spot_hedger()
    
    
    
    def delta_hedger():
        #Get present time                     
        present = datetime.utcnow()
        present = calendar.timegm(present.timetuple())
        present = present + 24*150 #add an hour to present
        present = datetime.fromtimestamp(present)
        
        #Get available futures/options contracts
        o_contracts = ws.get_instruments('BTC', kind='option', expired = False)
        o_contracts = pd.DataFrame(o_contracts['result'])
        
        f_contracts = ws.get_instruments('BTC', kind='future', expired = False)
        f_contracts = pd.DataFrame(f_contracts['result'])
        f_contracts = f_contracts[~f_contracts.instrument_name.str.contains("BTC-PERPETUAL")]
                 
        #Get option positions and open orders
        option_positions = ws.get_positions(currency='BTC', kind='option')
        option_positions = option_positions['result']
        option_positions[:] = [i for i in option_positions if i.get('direction') != 'zero']
        option_positions = pd.DataFrame(option_positions)  
        
        option_open_orders = ws.get_open_orders_by_currency(currency='BTC', kind='option')
        option_open_orders = option_open_orders['result']
        option_open_orders = pd.DataFrame(option_open_orders)  
        
        if option_positions.empty == True and option_open_orders.empty == True:
            print('we have no option positions or open option orders')
        
        elif option_positions.empty == False and option_open_orders.empty == True:
            o_n_positions = len(option_positions)
            print('we have ' + str(o_n_positions) + ' option positions and no open option orders')
            for instrument, direction, expiry, delta in zip(option_positions.instrument_name, option_positions.direction, o_contracts.expiration_timestamp, option_positions.delta):
                print('for our ' + instrument + ' ' + direction + ' option position ' + 'expiring on: ' + str(datetime.fromtimestamp(int(str(expiry)[:-3]))) + ' our delta is: ' + str(delta))
            total_o_delta = option_positions.delta.sum()
            print('our total option delta is: ' + str(total_o_delta))  
            
            o_positions = list(option_positions.instrument_name)
            o_contracts = o_contracts[o_contracts.instrument_name.str.contains('|'.join(o_positions))]
        
            print('')
            
            for i,c in zip(o_contracts.expiration_timestamp, o_contracts.instrument_name):
                if present > datetime.fromtimestamp(int(str(i)[:-3])):
                    print(c + ' your option contract is expiring at ' + str(datetime.fromtimestamp(int(str(i)[:-3]))))
                elif present < datetime.fromtimestamp(int(str(i)[:-3])):
                    print('none of our option contracts are expiring soon')
        
        elif option_positions.empty == False and option_open_orders.empty == False:
            o_n_positions = len(option_positions)
            o_n_orders = len(option_open_orders)
            print('we have ' + str(o_n_positions) + ' option positions and ' + str(o_n_orders) + ' open option orders')
            for instrument, direction, expiry, delta in zip(option_positions.instrument_name, option_positions.direction, o_contracts.expiration_timestamp, option_positions.delta):
                print('for our ' + instrument + ' ' + direction + ' option position ' + 'expiring on: ' + str(datetime.fromtimestamp(int(str(expiry)[:-3]))) + ' our delta is: ' + str(delta))
            total_o_delta = option_positions.delta.sum()
            print('our total option delta is: ' + str(total_o_delta))  
            
            o_positions = list(option_positions.instrument_name)
            o_contracts = o_contracts[o_contracts.instrument_name.str.contains('|'.join(o_positions))]
        
            print('')
            
            for i,c in zip(o_contracts.expiration_timestamp, o_contracts.instrument_name):
                if present > datetime.fromtimestamp(int(str(i)[:-3])):
                    print(c + ' your option contract is expiring at ' + str(datetime.fromtimestamp(int(str(i)[:-3]))))
                elif present < datetime.fromtimestamp(int(str(i)[:-3])):
                    print('none of our option contracts are expiring soon')
                    
        elif option_positions.empty == True and option_open_orders.empty == False:
            o_n_orders = len(option_open_orders)
            print('we have no option positions but ' + str(o_n_orders) + ' open option orders')
            for i in option_open_orders.instrument_name:
                print('we have an open option order on ' + i)
                
        print('')
        
        #Get future positions and open orders
        future_positions = ws.get_positions(currency='BTC', kind='future')
        future_positions = future_positions['result']
        future_positions[:] = [i for i in future_positions if i.get('direction') != 'zero']
        future_positions = pd.DataFrame(future_positions) 
        
        future_open_orders = ws.get_open_orders_by_currency(currency='BTC', kind='future')
        future_open_orders = future_open_orders['result']
        future_open_orders = pd.DataFrame(future_open_orders) 
        future_positions = future_positions[~future_positions.instrument_name.str.contains("BTC-PERPETUAL")]
        
        if future_positions.empty == True and future_open_orders.empty == True:
            print('we have no future positions or open future orders')
        
        elif future_positions.empty == False and future_open_orders.empty == True:
            o_f_positions = len(future_positions)
            print('we have ' + str(o_f_positions) + ' future positions and no open future orders')
            for instrument, direction, expiry, delta in zip(future_positions.instrument_name, future_positions.direction, f_contracts.expiration_timestamp, future_positions.delta):
                print('for our ' + instrument + ' ' + direction + ' future position ' + 'expiring on: ' + str(datetime.fromtimestamp(int(str(expiry)[:-3]))) + ' our delta is: ' + str(delta))
            total_f_delta = future_positions.delta.sum()
            print('our total future delta is: ' + str(total_f_delta))  
            
            f_positions = list(future_positions.instrument_name)
            f_contracts = f_contracts[f_contracts.instrument_name.str.contains('|'.join(f_positions))]
            
            print('')
            
            for i,c in zip(f_contracts.expiration_timestamp, f_contracts.instrument_name):
                if present > datetime.fromtimestamp(int(str(i)[:-3])):
                    print(c + ' your future contract is expiring at ' + str(datetime.fromtimestamp(int(str(i)[:-3]))))
                elif present < datetime.fromtimestamp(int(str(i)[:-3])):
                    print('no future contract is expiring soon')
        
        elif future_positions.empty == False and future_open_orders.empty == False:
            f_n_positions = len(future_positions)
            f_n_orders = len(future_open_orders)
            print('we have ' + str(f_n_positions) + ' future positions and ' + str(f_n_orders) + ' open future orders')
            for instrument, direction, expiry, delta in zip(future_positions.instrument_name, future_positions.direction, f_contracts.expiration_timestamp, future_positions.delta):
                print('for our ' + instrument + ' ' + direction + ' future position ' + 'expiring on: ' + str(datetime.fromtimestamp(int(str(expiry)[:-3]))) + ' our delta is: ' + str(delta))
            total_f_delta = future_positions.delta.sum()
            print('our total option delta is: ' + str(total_f_delta))  
            
            f_positions = list(future_positions.instrument_name)
            f_contracts = f_contracts[f_contracts.instrument_name.str.contains('|'.join(f_positions))]
            
            print('')
            
            for i,c in zip(f_contracts.expiration_timestamp, f_contracts.instrument_name):
                if present > datetime.fromtimestamp(int(str(i)[:-3])):
                    print(c + ' your future contract is expiring at ' + str(datetime.fromtimestamp(int(str(i)[:-3]))))
                elif present < datetime.fromtimestamp(int(str(i)[:-3])):
                    print('none of our future contracts are expiring soon')
                    
        elif future_positions.empty == True and future_open_orders.empty == False:
            f_n_orders = len(future_open_orders)
            print('we have no future positions but ' + str(f_n_orders) + ' open future orders')
            for i in future_open_orders.instrument_name:
                print('we have an open future order on ' + i)
                
        print('')
        
        
        #Loop over option positions to hedge
        if option_positions.empty == False:
            for i in range(0, o_n_positions):
                o_direction = option_positions.iloc[i, 16] #direction
                o_delta = option_positions.iloc[i, 17] #delta
                o_gamma = option_positions.iloc[i, 13] #gamma  
                o_price = option_positions.iloc[i, 18] #option price usd
                o_index = option_positions.iloc[i, 12] #index price
                o_size = option_positions.iloc[i, 3] #option position size
                hedge_usd = o_delta * o_index
                position_total_delta = o_delta #total option delta per position  
                
                o_instrument_name = option_positions.iloc[i, 10] #option contract
                f_instrument_name = o_instrument_name.rpartition('-')
                f_instrument_name = f_instrument_name[0].rpartition('-')
                f_instrument_name = f_instrument_name[0]
                
                f_contracts = ws.get_instruments('BTC', kind='future', expired = False)
                f_contracts = pd.DataFrame(f_contracts['result'])
                f_contracts = f_contracts[~f_contracts.instrument_name.str.contains("BTC-PERPETUAL")]  
                
                print('')
                print('finding corresponding future contract to use for ' + o_instrument_name)
                
                if f_instrument_name in list(f_contracts.instrument_name):
                    print('the contract to be used for hedging is: ' + f_instrument_name)
                    f_instrument_names = [f_instrument_name]
                elif f_instrument_name not in list(f_contracts.instrument_name):
                    print('the contract to be used for hedging is not in futures contract')
                    f_instrument_name = f_instrument_name.replace('BTC-', '')
                    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
                    for m in months:
                        if m in f_instrument_name:
                            month = m
                    m = (' ' + str(months.index(month) + 1) + ' ')
                    f_instrument_name = f_instrument_name.replace(month, m)
                    f_instrument_date = datetime.strptime(f_instrument_name, '%d %m %y')
                    
                    test = []
                    for i in f_contracts.instrument_name:
                        test.append(i.replace('BTC-', ''))
                    test2 = []
                    for i in test:
                        for m in months:
                            if m in i:
                                test2.append(i.replace(m, (' ' + str(months.index(m) + 1) + ' ')))   
                    f_instrument_dates = []
                    for i in test2:
                        f_instrument_dates.append(datetime.strptime(i, '%d %m %y'))
                    
                    try:
                        previous = max(dt for dt in f_instrument_dates if dt < f_instrument_date)
                    except ValueError:
                        print('no previous contract exists, only later ones')  
                    else:
                        print('previous contract exists')  
                        diff_previous_current = f_instrument_date - previous
                        diff_previous_current = diff_previous_current.days
                        n_previous = f_instrument_dates.index(previous)
                        print('the closest younger future contract to hedge our option position with is on: ' + f_contracts.instrument_name[n_previous])
                        print('with ' + str(diff_previous_current) + ' days difference between it and our current option contract')
                        
                        print('')
                    
                    try:
                        following = min(dt for dt in f_instrument_dates if dt > f_instrument_date)
                    except ValueError:
                        print('no later contract exists, only previous ones')  
                    else:
                        print('later contract exists')  
                        diff_current_following = following - f_instrument_date
                        diff_current_following = diff_current_following.days
                        n_following = f_instrument_dates.index(following)
                        print('the closest following future contract to hedge our option position with is on: ' + f_contracts.instrument_name[n_following])
                        print('with ' + str(diff_current_following) + ' days difference between it and our current option contract')
                        
                        print('')
                    
                    try:
                        f_instrument_names = [f_contracts.instrument_name[n_previous], f_contracts.instrument_name[n_following]]
                    except NameError:
                        try:
                            f_instrument_names = [f_contracts.instrument_name[n_previous]]
                        except NameError:
                            f_instrument_names = [f_contracts.instrument_name[n_following]]
                
                for f_instrument_name in f_instrument_names:        
                    #Get future positions and open orders
                    future_positions = ws.get_positions(currency='BTC', kind='future')
                    future_positions = future_positions['result']
                    future_positions[:] = [i for i in future_positions if i.get('direction') != 'zero']
                    future_positions = pd.DataFrame(future_positions) 
                    future_positions = future_positions[~future_positions.instrument_name.str.contains("BTC-PERPETUAL")]
                    
                    future_open_orders = ws.get_open_orders_by_currency(currency='BTC', kind='future')
                    future_open_orders = future_open_orders['result']
                    future_open_orders = pd.DataFrame(future_open_orders) 
                    
                    if future_open_orders.empty == True:
                        future_open_orders['instrument_name'] = []
                        
                    print('')    
                    print('hedging for ' + o_direction + ' position in: ' + o_instrument_name + ' with delta ' + str(o_delta))
                    
                    #if we have future positions
                    
                    if future_positions.empty == False and f_instrument_name in list(future_positions.instrument_name):
                        f_delta = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_name])
                        f_delta = f_delta.iloc[-1, -1]
                        hedge_usd = (f_delta + o_delta) * o_index
                        position_total_delta = o_delta + f_delta #total delta by combining future and option delta per position
                    
                    
                    if len(f_instrument_names) > 1 and f_instrument_names.index(f_instrument_name) == 0 and "n_following" in globals():
                        if f_instrument_names[1] in list(future_positions.instrument_name):
                            if f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_following
                            elif f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta1 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[0]])
                                f_delta1 = f_delta1.iloc[-1, -1]
                                f_delta2 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[1]])
                                f_delta2 = f_delta2.iloc[-1, -1]
                                hedge_usd = ((f_delta1 + f_delta2 + o_delta) * o_index)/n_following
                                position_total_delta = f_delta1 + f_delta2 + o_delta 
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_following
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta1 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[0]])
                                f_delta1 = f_delta1.iloc[-1, -1]
                                f_delta2 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[1]])
                                f_delta2 = f_delta2.iloc[-1, -1]
                                hedge_usd = ((f_delta1 + f_delta2 + o_delta) * o_index)/n_following
                                position_total_delta = f_delta1 + f_delta2 + o_delta 
                        elif f_instrument_names[1] not in list(future_positions.instrument_name):
                            if f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_following
                            elif f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_name])
                                f_delta = f_delta.iloc[-1, -1]
                                hedge_usd = ((f_delta + o_delta) * o_index)/n_following
                                position_total_delta =  f_delta + o_delta 
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_following
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_name])
                                f_delta = f_delta.iloc[-1, -1]
                                hedge_usd = ((f_delta + o_delta) * o_index)/n_following
                                position_total_delta = f_delta + o_delta 
                            
                    if len(f_instrument_names) > 1 and f_instrument_names.index(f_instrument_name) != 0 and "n_previous" in globals():
                        if f_instrument_names[0] in list(future_positions.instrument_name):
                            if f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_previous
                            elif f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta1 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[0]])
                                f_delta1 = f_delta1.iloc[-1, -1]
                                f_delta2 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[1]])
                                f_delta2 = f_delta2.iloc[-1, -1]
                                hedge_usd = ((f_delta1 + f_delta2 + o_delta) * o_index)/n_previous
                                position_total_delta = f_delta1 + f_delta2 + o_delta 
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_previous
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta1 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[0]])
                                f_delta1 = f_delta1.iloc[-1, -1]
                                f_delta2 = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_names[1]])
                                f_delta2 = f_delta2.iloc[-1, -1]
                                hedge_usd = ((f_delta1 + f_delta2 + o_delta) * o_index)/n_previous
                                position_total_delta = f_delta1 + f_delta2 + o_delta 
                        elif f_instrument_names[0] not in list(future_positions.instrument_name):
                            if f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_previous
                            elif f_instrument_name not in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_name])
                                f_delta = f_delta.iloc[-1, -1]
                                hedge_usd = ((f_delta + o_delta) * o_index)/n_previous
                                position_total_delta =  f_delta + o_delta 
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name not in list(future_positions.instrument_name):
                                hedge_usd = hedge_usd/n_following
                            elif f_instrument_name in list(future_open_orders.instrument_name) and f_instrument_name in list(future_positions.instrument_name):
                                f_delta = pd.DataFrame(future_positions.delta[future_positions.instrument_name == f_instrument_name])
                                f_delta = f_delta.iloc[-1, -1]
                                hedge_usd = ((f_delta + o_delta) * o_index)/n_previous
                                position_total_delta = f_delta + o_delta 
                    
                    def round(n):
                        a = (n // 10) * 10                   # Smaller multiple
                        b = a + 10                           # Larger multiple
                        return (b if n - a > b - n else a)   # Return of closest of two
                    hedge_usd = round(hedge_usd)
                    
                    
                    #DO NOTHING
                    if position_total_delta < 0.01 and position_total_delta > -0.01:
                        print('total delta for position in this maturity is: ' + str(position_total_delta) + ' so we dont have to hedge')
                        print('-------------------------------------------------------------------------------')
                        
                    #SHORT FUTURE
                    elif position_total_delta > 0.01:
                        print('total delta for position in this maturity is: ' + str(position_total_delta) + ' so we have to hedge')
                        
                        if f_instrument_name in list(future_open_orders.instrument_name):
                        #EDIT SHORT POSITONS
                            orderbook = ws.get_orderbook(f_instrument_name)
                            orderbook = orderbook['result']
                            order_id = future_open_orders.order_id[future_open_orders.instrument_name == f_instrument_name]
                            #if str(future_open_orders.direction[future_open_orders.instrument_name == f_instrument_name][0]) == 'sell':
                            order = ws.edit_order(str(order_id[0]), abs(hedge_usd), orderbook['mark_price'])
                            print('SELL limit order was edited for ' + str(abs(hedge_usd)) + ' USD on ' + f_instrument_name + ' at price ' + str(orderbook['index_price']))
                        
                        elif f_instrument_name not in list(future_open_orders.instrument_name) and o_gamma > 0:
                            print('gamma is: ' + str(o_gamma) + ' for this position, so we place limit orders')
                            print('-------------------------------------------------------------------------------')
                            orderbook = ws.get_orderbook(f_instrument_name)
                            orderbook = orderbook['result']
                            order = ws.limit_order(f_instrument_name, abs(hedge_usd), 'short', orderbook['mark_price'], post_only=True, reduce_only=False) #long or short
                            print('SELL limit order was sent for ' + str(abs(hedge_usd)) + ' USD on ' + f_instrument_name)
    
                        elif f_instrument_name not in list(future_open_orders.instrument_name) and o_gamma <= 0:
                            print('gamma is: ' + str(o_gamma) + ' for this position, so we place market orders')
                            print('-------------------------------------------------------------------------------')
                            order = ws.market_order(f_instrument_name, abs(hedge_usd), 'short') #long or short
                            print('SELL market order was sent for ' + str(abs(hedge_usd)) + ' USD on ' + f_instrument_name)
                        
                        order = pd.DataFrame(order)
                        #check if order was filled and sent
                        if order.columns[2] == 'error':
                            order = list(order.error)   
                            reason = order[1]['reason']
                            print('order was not sent because it ' + reason)
                        elif order.columns[2] == 'result':     
                            order = list(order.result) 
                            while True:
                                order_id = order[0]['order_id']
                                order_state = ws.get_order_state(order_id)
                                order_type = order_state['result']['order_type']
                                order_state = order_state['result']['order_state']
                                if order_state != 'open' and order_type == 'market':
                                    print('market order was ' + order_state)
                                    break
                                elif order_state != 'open' and order_type == 'limit':
                                    print('limit order was ' + order_state)
                                    break
                                elif order_state == 'open' and order_type == 'market':
                                    print('market order is ' + order_state + ' and waiting to get filled')
                                    sleep(10)
                                    order_state = ws.get_order_state(order_id)
                                    order_type = order_state['result']['order_type']
                                    order_state = order_state['result']['order_state']
                                    if order_state == 'open' and order_type == 'market':
                                        ws.cancel(order_id)
                                        print('market order was not filled, so it got cancelled')
                                        break           
                                elif order_state == 'open' and order_type == 'limit':
                                    print('limit order is ' + order_state + ' and waiting to get filled')
                                    sleep(10)
                                    order_state = ws.get_order_state(order_id)
                                    order_type = order_state['result']['order_type']
                                    order_state = order_state['result']['order_state']
                                    if order_state == 'open' and order_type == 'limit':
                                        print('limit order was not filled but it was edited successfully to hedge delta once it is')
                                        break                
    
                            #check delta of future contract
                            future_positions = ws.get_position(f_instrument_name)
                            future_positions = future_positions['result']
                            if future_positions['direction'] == 'zero':
                                future_positions = []
                            future_positions = pd.DataFrame([future_positions])
    
                            
                            future_open_orders = ws.get_open_orders_by_instrument(f_instrument_name)
                            future_open_orders = future_open_orders['result']
                            future_open_orders = pd.DataFrame(future_open_orders) 
    
                            if future_positions.empty == False and future_open_orders.empty == True:
                                print('we have filled positions and no open orders on ' + f_instrument_name)
                                new_f_delta = pd.DataFrame(future_positions.delta)
                                new_f_delta = new_f_delta.iloc[-1, -1]
                                print('new delta for futures in this maturity: ' + str(new_f_delta))
                                new_position_total_delta = o_delta + new_f_delta
                                print('new total delta for positions in this maturity: ' + str(new_position_total_delta))
                            elif future_positions.empty == False and future_open_orders.empty == False:
                                print('we have filled positions and open orders on ' + f_instrument_name)
                                new_f_delta = pd.DataFrame(future_positions.delta)
                                new_f_delta = new_f_delta.iloc[-1, -1]
                                print('new delta for futures in this maturity: ' + str(new_f_delta))
                                new_position_total_delta = o_delta + new_f_delta
                                print('new total delta for positions in this maturity: ' + str(new_position_total_delta))
                            elif future_positions.empty == True and future_open_orders.empty == False:
                                print('we have no filled positions but we have open orders waiting to be filled on ' + f_instrument_name)
                            elif future_positions.empty == True and future_open_orders.empty == True:
                                print('we have no filled positions or open orders on ' + f_instrument_name)
    
                    #BUY FUTURE
                    elif position_total_delta < -0.01:
                        print('total delta for position in this maturity is: ' + str(position_total_delta) + ' so we have to hedge')
                        
                        if f_instrument_name in list(future_open_orders.instrument_name):
                            #EDIT BUY POSITONS
                            orderbook = ws.get_orderbook(f_instrument_name)
                            orderbook = orderbook['result']
                            order_id = future_open_orders.order_id[future_open_orders.instrument_name == f_instrument_name]
                            #if str(future_open_orders.direction[future_open_orders.instrument_name == f_instrument_name][0]) == 'buy':
                            order = ws.edit_order(str(order_id[0]), abs(hedge_usd), orderbook['mark_price'])
                            print('BUY limit order was edited for ' + str(abs(hedge_usd)) + ' USD on ' + f_instrument_name + ' at price ' + str(orderbook['index_price']))
    
                        elif f_instrument_name not in list(future_open_orders.instrument_name) and o_gamma > 0:
                            print('gamma is: ' + str(o_gamma) + ' for this position, so we place limit orders')
                            print('-------------------------------------------------------------------------------')
                            orderbook = ws.get_orderbook(f_instrument_name)
                            orderbook = orderbook['result']
                            order = ws.limit_order(f_instrument_name, abs(hedge_usd), 'long', orderbook['mark_price'], post_only=True, reduce_only=False) #long or short
                            print('BUY limit order was sent for ' + str(abs(hedge_usd)) + ' USD on ' + f_instrument_name)
    
                        elif f_instrument_name not in list(future_open_orders.instrument_name) and o_gamma <= 0:
                            print('gamma is: ' + str(o_gamma) + ' for this position, so we place market orders')
                            print('-------------------------------------------------------------------------------')
                            order = ws.market_order(f_instrument_name, abs(hedge_usd), 'long') #long or short
                            print('BUY market order was sent for ' + str(abs(hedge_usd)) + ' USD on ' + f_instrument_name)
                        
                        order = pd.DataFrame(order)
                        #check if order was filled and sent
                        if order.columns[2] == 'error':
                            order = list(order.error)   
                            reason = order[1]['reason']
                            print('order was not sent because it ' + reason)
                        elif order.columns[2] == 'result':     
                            order = list(order.result) 
                            while True:
                                order_id = order[0]['order_id']
                                order_state = ws.get_order_state(order_id)
                                order_type = order_state['result']['order_type']
                                order_state = order_state['result']['order_state']
                                if order_state != 'open' and order_type == 'market':
                                    print('market order was ' + order_state)
                                    break
                                elif order_state != 'open' and order_type == 'limit':
                                    print('limit order was ' + order_state)
                                    break
                                elif order_state == 'open' and order_type == 'market':
                                    print('market order is ' + order_state + ' and waiting to get filled')
                                    sleep(10)
                                    order_state = ws.get_order_state(order_id)
                                    order_type = order_state['result']['order_type']
                                    order_state = order_state['result']['order_state']
                                    if order_state == 'open' and order_type == 'market':
                                        ws.cancel(order_id)
                                        print('market order was not filled, so it got cancelled')
                                        break           
                                elif order_state == 'open' and order_type == 'limit':
                                    print('limit order is ' + order_state + ' and waiting to get filled')
                                    sleep(10)
                                    order_state = ws.get_order_state(order_id)
                                    order_type = order_state['result']['order_type']
                                    order_state = order_state['result']['order_state']
                                    if order_state == 'open' and order_type == 'limit':
                                        print('limit order was not filled but it was edited successfully to hedge delta once it is')
                                        break                
    
                            #check delta of future contract
                            future_positions = ws.get_position(f_instrument_name)
                            future_positions = future_positions['result']
                            if future_positions['direction'] == 'zero':
                                future_positions = []
                            future_positions = pd.DataFrame([future_positions])
                            
                            future_open_orders = ws.get_open_orders_by_instrument(f_instrument_name)
                            future_open_orders = future_open_orders['result']
                            future_open_orders = pd.DataFrame(future_open_orders) 
    
                            if future_positions.empty == False and future_open_orders.empty == True:
                                print('we have filled positions and no open orders on ' + f_instrument_name)
                                new_f_delta = pd.DataFrame(future_positions.delta)
                                new_f_delta = new_f_delta.iloc[-1, -1]
                                print('new delta for futures in this maturity: ' + str(new_f_delta))
                                new_position_total_delta = o_delta + new_f_delta
                                print('new total delta for positions in this maturity: ' + str(new_position_total_delta))
                            elif future_positions.empty == False and future_open_orders.empty == False:
                                print('we have filled positions and open orders on ' + f_instrument_name)
                                new_f_delta = pd.DataFrame(future_positions.delta)
                                new_f_delta = new_f_delta.iloc[-1, -1]
                                print('new delta for futures in this maturity: ' + str(new_f_delta))
                                new_position_total_delta = o_delta + new_f_delta
                                print('new total delta for positions in this maturity: ' + str(new_position_total_delta))
                            elif future_positions.empty == True and future_open_orders.empty == False:
                                print('we have no filled positions but we have open orders waiting to be filled on ' + f_instrument_name)
                            elif future_positions.empty == True and future_open_orders.empty == True:
                                print('we have no filled positions or open orders on ' + f_instrument_name)
    
        print('')
        print('')
        print('')
    
                
        #get new portfolio 
        future_positions = ws.get_positions(currency='BTC', kind='future')
        future_positions = future_positions['result']
        future_positions[:] = [i for i in future_positions if i.get('direction') != 'zero']
        future_positions = pd.DataFrame(future_positions) 
        
        future_open_orders = ws.get_open_orders_by_currency(currency='BTC', kind='future')
        future_open_orders = future_open_orders['result']
        future_open_orders = pd.DataFrame(future_open_orders)
        
        summary = ws.account_summary(currency='BTC')
        summary = pd.DataFrame(summary['result'])
    
        if future_positions.empty == False:
            for i in future_positions.instrument_name:
                print('we now have filled future positions on ' + i)
        
            #calculate new delta 
            o_delta = option_positions.delta.sum()
            f_delta = future_positions[~future_positions.instrument_name.str.contains("BTC-PERPETUAL")].delta.sum()
            future_positions = future_positions[future_positions.instrument_name.str.contains("BTC-PERPETUAL")]
            if future_positions.empty == False:
                spot_delta = summary.equity[0] + float(future_positions['delta'])
                portfolio_delta = (o_delta + f_delta) + spot_delta
                delta_total = summary.equity[0] + summary.delta_total[0]
                print('with all our new positions, our option delta is: ' + str(o_delta))
                print('with all our new positions, our future delta is: ' + str(f_delta))
                print('with all our new positions, our spot delta is: ' + str(spot_delta))
                print('with all our new positions, our portfolio delta is: ' + str(portfolio_delta))            
                print('deribits equity + delta total says: ' + str(delta_total))   
            
            elif future_positions.empty == True:
                print('no perpetual positions to hedge spot')

            
        if future_open_orders.empty == False:
            for i in future_open_orders.instrument_name:
                print('we now have open future orders on ' + i)
              
        if future_positions.empty == True and future_open_orders.empty == True:
            print('delta hedger did not hedge')
        return
       
    delta_hedger()
    



