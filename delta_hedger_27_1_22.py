#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 27 9:20:34 2022

@author: manu
"""


import asyncio
import websockets
import json
import requests
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import nest_asyncio
import glom
import datetime
from datetime import timezone
import calendar
from datetime import datetime
from time import sleep
from operator import itemgetter
nest_asyncio.apply()


currency = 'BTC'
currency_small = 'btc'
f_instrument_name = f'{currency}-25MAR22'.format(currency)

threshold_up = 0.2
threshold_down = -0.2
order_destination = 'mark_price'
pct_diff = 3
panic_gamma = 'off'


def platform_and_creds(platform):
    if platform == 'mainnet':
        platform = "wss://www.deribit.com/ws/api/v2/"
        client_id = "HZOjb73g"
        client_secret = "vUQOrtOnNmDu7o91z_FUoPKC5JiothNtAjGiY0NJogM"
    else:
        platform = "wss://test.deribit.com/ws/api/v2/"
        client_id = "HZOjb73g"
        client_secret = "vUQOrtOnNmDu7o91z_FUoPKC5JiothNtAjGiY0NJogM"
    return platform, client_id, client_secret

platform_and_creds = platform_and_creds('testnet')

platform = platform_and_creds[0]
client_id = platform_and_creds[1]
client_secret = platform_and_creds[2]

url = 'https://www.deribit.com/api/v2/'

auth_creds = {
              "jsonrpc" : "2.0",
              "id" : 43,
              "method" : "public/auth",
              "params" : {
                "grant_type" : "client_credentials",
                "client_id" : client_id,
                "client_secret" : client_secret
              }
            }

msg = {
       "jsonrpc": "2.0",
       "id": 0,
       "method": None,
      }

async def priv_api(msg):
    async with websockets.connect(platform) as websocket:
            await websocket.send(json.dumps(auth_creds))
            while websocket.open:
                response = await websocket.recv()
                await websocket.send(msg)
                response = await websocket.recv()
                break
            return json.loads(response)

def async_loop(api, message):
        return asyncio.get_event_loop().run_until_complete(api(message))

def test_creds():
        response = async_loop(priv_api, json.dumps(auth_creds))
        if 'error' in response.keys():
            raise Exception(f"Auth failed with error {response['error']}")
        else:
            print("Auth creds are good, it worked")

test_creds()

#BEGIN STREAM WITH ADDED CHANNELS

def channels():
    msgplatformstate = {
                "jsonrpc": "2.0",
                "id": 43,
                "method": "public/subscribe",
                "params":{"channels": ["platform_state"]}
        }
    
    msgportfolio = {
                "jsonrpc": "2.0",
                "id": 43,
                "method": "private/subscribe",
                "params":{"channels": ["user.portfolio."f'{currency_small}'.format(currency_small)]}
        }
    
    msgopenorders = {
                "jsonrpc": "2.0",
                "id": 43,
                "method": "private/subscribe",
                "params":{"channels": ["user.orders."f'{f_instrument_name}.raw'.format(f_instrument_name)]}
        }
    
    msgchanges = {
                "jsonrpc": "2.0",
                "id": 43,
                "method": "private/subscribe",
                "params":{"channels": ["user.changes."f'{f_instrument_name}.raw'.format(f_instrument_name)]}
        }
    return msgplatformstate, msgportfolio, msgopenorders, msgchanges

channel = channels()

msgplatformstate = channel[0]
msgportfolio = channel[1]
msgopenorders = channel[2]
msgchanges = channel[3]

# create the lists to be used to store streaming values
s_delta = [0]
s_equity = [0]
s_price = [0]
s_amount = [0]
open_delta = []
change = [0] 
saved_delta = [0]

s_trade_changes = []
s_position_changes = []
s_order_changes = []
s_account_changes = [False]


#calls the websocket
async def call_websocket(msgplatformstate, msgchanges, msgportfolio, msgopenorders):
    async with websockets.connect(platform) as websocket:
        global saved_delta
        global change
        global s_trade_changes
        global s_position_changes
        global s_order_changes
        global s_account_changes
        await websocket.send(json.dumps(auth_creds))
        await websocket.send(msgplatformstate)
        await websocket.send(msgchanges)   
        await websocket.send(msgportfolio)            
        await websocket.send(msgopenorders)            
        while websocket.open:
            response = await websocket.recv()
            output = json.loads(response)
            if "result" in output.keys(): 
                print("Connection successful")
            if "params" in output.keys():                    
                
                if output["params"]["channel"] == "platform_state":
                    state = output["params"]["data"]["locked"]
                    if state == False:
                        print('Platform state is operative')
                        print('')
                        pass
                    else:
                        print('Platform state is locked')
                        print('')
                        break
                if output["params"]["channel"] == "user.changes."f'{f_instrument_name}.raw'.format(f_instrument_name): 
                    s_trade_changes.append(output["params"]["data"]["trades"])
                    s_position_changes.append(output["params"]["data"]["positions"])
                    s_order_changes.append(output["params"]["data"]["orders"])
                if output["params"]["channel"] == "user.portfolio."f'{currency_small}'.format(currency_small): 
                    s_delta.append(output["params"]["data"]["delta_total"])
                    s_equity.append(output["params"]["data"]["equity"])
                if output["params"]["channel"] == "user.orders."f'{f_instrument_name}.raw'.format(f_instrument_name): 
                    s_price.append(output["params"]["data"]["price"])
                    s_amount.append(output["params"]["data"]["amount"])
                    
                
                
                if s_trade_changes != [] or s_position_changes != [] or s_order_changes != []:
                    if s_order_changes[0][0]['replaced'] == False:
                        print('Changes in orders are reported. We have to hedge')
                        s_account_changes.append(True)
                        print('')
                        pass
                    else:
                        print('No changes in orders are reported. Continue...')
                        s_trade_changes = []
                        s_position_changes = []
                        s_order_changes = []
                        s_account_changes.append(False)
                        print('')
                        pass
                else:
                    print('No changes in orders are reported. Continue...')
                    s_trade_changes = []
                    s_position_changes = []
                    s_order_changes = []
                    s_account_changes.append(False)
                    print('')
                    pass
                
                if float(s_amount[-1]) == 0 and float(s_price[-1]) == 0:
                    open_delta.append(0)
                else:
                    open_delta.append(float(s_amount[-1]/s_price[-1]))
                
                current_delta = [s_equity[-1] + s_delta[-1] - open_delta[-1]]
                change.append((current_delta[-1] - saved_delta[-1]))
                print('Current Delta: ' + str(current_delta[-1])) 
                print('')
                print('Saved Delta: ' + str(saved_delta[-1])) 
                print('')
                print('Open Delta: ' + str(open_delta[-1])) 
                print('')
                print('Change: ' + str(change))
                print('')
                print('Position change checker: ' + str(s_account_changes[-1]))
                print('')

                if saved_delta == [0]:
                    if current_delta == [0]:
                        change = [0]
                        pass
                    elif current_delta != [0]:
                        saved_delta.append(current_delta[-1])
                        change = [0]
                    
                #if last_value has an input (stream started), calculate its last value with the latest one to account for change.
                elif current_delta[-1] > threshold_up or current_delta[-1] < threshold_down or change[-1] > threshold_up or change[-1] < threshold_down or s_account_changes[-1] == True: #if change is higher than threshold, run delta hedger
                    print('')   
                    print('Change in saved and current deltas broke threshold or there has been a change in an order, so we have to hedge')
                    print('==================================================')
                    print('')
                    
                    saved_delta = [0]
                    current_delta = [0]
                    change = [0]
    
                        
#asyncio.get_event_loop().run_until_complete(call_websocket(json.dumps(msgplatformstate), json.dumps(msgchanges), json.dumps(msgportfolio),json.dumps(msgopenorders)))
#print("Finished")
                            
#uncomment above if you want to test out the stream


    
    
                    def Public_API(method, params):
                        webdata = requests.get(url + "public/" + method, params)
                        return webdata.json()['result']
                
                    def Private_API(method, params):
                        msg = {"jsonrpc" : "2.0", "id" : 2515, "method" : "private/" + method, "params" : params}
                        response = async_loop(priv_api, json.dumps(msg))
                        return response

                    price = Public_API(method = 'get_index', params = {"currency" : currency})[currency]
                    summary = pd.DataFrame(Private_API(method = 'get_account_summary', params = {"currency": currency, "extended": True}))

                    future_open_orders = Private_API(method = 'get_open_orders_by_currency', params = {"currency": currency, "kind": 'future'})['result']
                    future_open_orders[:] = [i for i in future_open_orders if i.get('direction') != 'zero']
                    future_open_orders = pd.DataFrame(future_open_orders) 
                    if future_open_orders.empty == True:
                        future_open_orders = []
                        future_open_orders = pd.DataFrame(future_open_orders)
                        future_open_orders['instrument_name'] = []
                        future_open_orders['order_id'] = []
                        order_delta = 0
                    if future_open_orders.empty == False:
                        order_delta = float(sum(future_open_orders['amount']/future_open_orders['price']))
                    
                    equity = summary['result'][9]
                    book_delta = summary['result'][5]
                    order_delta = order_delta
                    
                    delta_incomplete = equity + book_delta
                    delta_total = equity + book_delta + order_delta 
                    gamma_total = summary['result'][23] 
                    
                    if delta_incomplete < threshold_up and delta_incomplete > threshold_down:
                        print('Unfilled delta: ' + str(delta_total))
                        print('Filled delta: ' + str(delta_incomplete))                    
                    
                    else:
                        hedge_usd = delta_total * price
                        def round(n):
                            a = (n // 10) * 10                   
                            b = a + 10                           
                            return (b if n - a > b - n else a)   
                        hedge_usd = abs(round(hedge_usd))
                        
                        def directions(delta_total):
                            if delta_total > threshold_up:
                                direction = "sell"
                                counter_direction = "buy"
                            else:
                                direction = "buy"
                                counter_direction = "sell"
                            return direction, counter_direction
                        
                        directions = directions(delta_total)
                        direction = directions[0]
                        counter_direction = directions[1]

                        def order_type():
                            if panic_gamma == 'off' and direction == 'buy':
                                order_type_a = "limit"
                                order_type_b = "take_limit"
                                order_type_c = "limit"
                            elif panic_gamma == 'off' and direction == 'sell':
                                order_type_a = "limit"
                                order_type_b = "take_limit"
                                order_type_c = "limit"
                            elif panic_gamma == 'on' and direction == 'buy':
                                order_type_a = "market"
                                order_type_b = "take_market"
                                order_type_c = "market"
                            elif panic_gamma == 'on' and direction == 'sell':
                                order_type_a = "market"
                                order_type_b = "take_market"
                                order_type_c = "market"
                            return order_type_a, order_type_b, order_type_c
                        
                        order_types = order_type()
                        order_type_a = order_types[0]
                        order_type_b = order_types[1]
                        order_type_c = order_types[2]
                        
                        orderbook = Public_API(method = 'get_order_book', params = {"instrument_name" : f_instrument_name})
                        
                        P = abs(int(orderbook[order_destination]))
                        H = abs(hedge_usd)
                        D = delta_total
                        G = gamma_total * 100
                        
                        def order_calculation():
                         
                            P1 = abs(int(P + (P * pct_diff/100)))
                            P2 = abs(int(P1 + ((P1 * (pct_diff))/100)))
                            P3 = abs(int(P2 + ((P2 * (pct_diff))/100)))
                            Ps = [P1, P2, P3]
                            
                            P_1 = abs(int(P - (P * pct_diff/100)))
                            P_2 = abs(int(P_1 - ((P_1 * (pct_diff))/100)))
                            P_3 = abs(int(P_2 - ((P_2 * (pct_diff))/100)))
                            P_s = [P_1, P_2, P_3]
                        
                            D1 = D + G                            
                            G1 = (D - D1) / (P - P1) * 100
                            D2 = D1 + G1
                            G2 = (D1 - D2) / (P1 - P2) * 100
                            D3 = D2 + G2
                            G3 = (D2 - D3) / (P2 - P3) * 100
                            
                            D_1 = D - G                            
                            G_1 = (D + D_1) / (P + P_1) * 100
                            D_2 = D_1 - G_1
                            G_2 = (D_1 + D_2) / (P_1 + P_2) * 100
                            D_3 = D_2 - G_2
                            G_3 = (D_2 + D_3) / (P_2 + P_3) * 100
                            
                            H1 = abs(round((P1 * (D1 + G1)) - H))
                            H2 = abs(round((P2 * (D2 + G2)) - (P1 * (D1 + G1))))
                            H3 = abs(round((P3 * (D3 + G3)) - (P2 * (D2 + G2))))
                            Hs = [H1, H2, H3]

                            H_1 = abs(round((P_1 * (D_1 + G_1)) - H))
                            H_2 = abs(round((P_2 * (D_2 + G_2)) - (P_1 * (D_1 + G_1))))
                            H_3 = abs(round((P_3 * (D_3 + G_3)) - (P_2 * (D_2 + G_2))))
                            H_s = [H_1, H_2, H_3]
                            
                            return Ps, P_s, Hs, H_s
                        
                        order_calculation = order_calculation()
                        Ps = order_calculation[0]
                        P_s = order_calculation[1]
                        Hs = order_calculation[2]
                        H_s = order_calculation[3]

                        orders = []
                        
                        def delta_hedger():
                            for i in future_open_orders.order_id:
                                Private_API(method = 'cancel', params = {"order_id" : i})
                                print(i + ' was cancelled')

                            Private_API(method = direction, params = {"instrument_name": f_instrument_name, "amount": H, "type": order_type_a, 'price': P})
                            print('MAIN ' + direction + ' ' + order_type_a + ' order was SENT at ' + str(P) + ' for ' + str(H) + ' USD on ' + f_instrument_name)
                            
                            if direction == 'buy':
                                for p_s, h_s in zip(P_s, H_s):
                                    orders.append(Private_API(method = direction, params = {"instrument_name": f_instrument_name, "amount": h_s, "type": order_type_c, 'price': p_s}))
                                    print(direction + " " + order_type_b + ' order was SENT at ' + str(p_s) + ' for ' + str(h_s) + ' USD on ' + f_instrument_name)
                                    
                                for ps, hs in zip(Ps, Hs):
                                    orders.append(Private_API(method = counter_direction, params = {"instrument_name": f_instrument_name, "amount": hs, "type": order_type_b, 'price': ps, 'trigger_price': ps, 'trigger': order_destination}))
                                    print(counter_direction + ' ' + order_type_c + ' order was SENT at ' + str(ps) + ' for ' + str(hs) + ' USD on ' + f_instrument_name)
                                
                            elif direction == 'sell':
                                for ps, hs in zip(Ps, Hs):
                                    orders.append(Private_API(method = direction, params = {"instrument_name": f_instrument_name, "amount": hs, "type": order_type_c, 'price': ps}))
                                    print(direction + ' ' + order_type_c + ' order was SENT at ' + str(ps) + ' for ' + str(hs) + ' USD on ' + f_instrument_name)
                                    
                                for p_s, h_s in zip(P_s, H_s):
                                    orders.append(Private_API(method = counter_direction, params = {"instrument_name": f_instrument_name, "amount": h_s, "type": order_type_b, 'price': p_s, 'trigger_price': p_s+10, 'trigger': order_destination}))
                                    print(counter_direction + " " + order_type_b + ' order was SENT at ' + str(p_s+10) + ' for ' + str(h_s) + ' USD on ' + f_instrument_name)

                            #check where it went and what happened to it
                            for i in orders:
                                i = pd.DataFrame(i)
                                if i.columns[2] == 'error':
                                    i = list(i.error)   
                                    reason = i[1]
                                    try:
                                        print('order was not sent because it ' + reason)
                                    except TypeError:
                                        pass
                                elif i.columns[2] == 'result':     
                                    i = list(i.result) 
                                    order_id = i[0]['order_id']
                                    order_state = Private_API(method = 'get_order_state', params = {"order_id": order_id})['result']['order_state']
                                    if order_state == 'untriggered' or order_state == 'open':
                                        print('order is ' + order_state)
                                    elif order_state == 'filled':
                                        print('order is ' + order_state)
                                    elif order_state != 'untriggered' or order_state != 'open' or order_state != 'filled':
                                        print('order was ' + order_state)
                                        
                            return 
                        
                        delta_hedger()
                    sleep(5)
    
asyncio.get_event_loop().run_until_complete(call_websocket(json.dumps(msgplatformstate), json.dumps(msgchanges), json.dumps(msgportfolio), json.dumps(msgopenorders)))
print('Finished')