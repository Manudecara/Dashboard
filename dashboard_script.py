# In[1]:

import datetime as dt
from dateutil import parser
import math
import sys
import numpy as np
from numpy import cumsum, log, polyfit, sqrt, std, subtract
from scipy.stats import probplot, moment
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.stattools import acf, q_stat, adfuller
import arch
from arch import arch_model
import warnings
import requests
import json
import websockets
import asyncio
import nest_asyncio
from fpdf import fpdf
import os
import shutil

nest_asyncio.apply()


# In[2]:

class DeribitOptionsData:
        def __init__(self, instrument):
            instrument = instrument.upper()
            if instrument not in ['BTC', 'ETH']:
                raise ValueError('instrument must be either BTC or ETH')
            self._instrument = instrument
            self.options = None
            self._get_options_chain()
            self.call_time = dt.datetime.now()
            
        @staticmethod
        async def call_api(msg):
            async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
                await websocket.send(msg)
                while websocket.open:
                    response = await websocket.recv()
                    return response
        
        @staticmethod
        def json_to_dataframe(response):
            response = json.loads(response)
            results = response['result']
            df = pd.DataFrame(results)
            return df
        
        def update(self):
            self._get_options_chain()
        
        @property
        def instrument(self):
            return self._instrument

        @instrument.setter
        def instrument(self, new_instrument):
            if isinstance(new_instrument, str):
                self._instrument = new_instrument
            else:
                raise ValueError('New instrument must be a string')
                
        @staticmethod
        def date_parser(str_date):
            date = str_date.split('-')[-1]
            return parser.parse(date)

        @staticmethod
        def strike_parser(inst_name):
            strike = inst_name.split('-')[-2]
            return int(strike)

        @staticmethod
        def side_parser(inst_name):
            side = inst_name.split('-')[-1]
            if side == 'P':
                return 'Put'
            if side == 'C':
                return 'Call'
            else:
                return 'N/A'
        
        @staticmethod
        def async_loop(message):
            return asyncio.get_event_loop().run_until_complete(DeribitOptionsData.call_api(message))

        def process_df(self, df):
            df['expiry'] = [DeribitOptionsData.date_parser(date) for date in df.underlying_index]  
            df['strike'] = [DeribitOptionsData.strike_parser(i) for i in df.instrument_name]
            df['type'] = [DeribitOptionsData.side_parser(j) for j in df.instrument_name]
            df['dollar_bid'] = df.underlying_price * df.bid_price
            df['dollar_ask'] = df.underlying_price * df.ask_price
            df['dollar_mid'] = df.underlying_price * df.mid_price
            df['time'] = (df['expiry'] - dt.datetime.now()).dt.days /365
            return df
        
        @staticmethod
        def get_quote(instrument):
            msg1 =                 {
                    "jsonrpc": "2.0",
                    "id": 9344,
                    "method": "public/ticker",
                    "params": {
                        "instrument_name": instrument +'-PERPETUAL',
                        "kind": "future"
                    }
                }
            quote = json.loads(DeribitOptionsData.async_loop(json.dumps(msg1)))
            return float(quote['result']['last_price'])
        
        @property
        def chain(self):
            return self.options
        
        def _get_options_chain(self):
            msg1 =                 {
                    "jsonrpc": "2.0",
                    "id": 9344,
                    "method": "public/get_book_summary_by_currency",
                    "params": {
                        "currency": self._instrument,
                        "kind": "option"
                    }
                }

            response = self.async_loop(json.dumps(msg1))
            df = self.json_to_dataframe(response)
            df = self.process_df(df)
            self.options = df.copy(deep=True)

        def available_instruments(self, currency, expired=False):
            msg =                 {
                    "jsonrpc": "2.0",
                    "id": 9344,
                    "method": "public/get_instruments",
                    "params": {
                        "currency": currency,
                        "kind": "option",
                        "expired": expired
                    }
                }
            resp = self.async_loop(json.dumps(msg))
            resp = json.loads(resp)
            instruments = [d["instrument_name"] for d in resp['result']]
            return instruments
        
        @classmethod
        def option_info(cls, option_label):
            msg =                 {
                    "jsonrpc": "2.0",
                    "id": 8106,
                    "method": "public/ticker",
                    "params": {
                        "instrument_name": option_label
                    }
                }

            response = DeribitOptionsData.async_loop(json.dumps(msg))
            return json.loads(response)

        def expiries(self):
            return sorted(self.options.expiry.unique())

        def get_side_exp(self, side, exp='all'):
            if side.capitalize() not in ['Call', 'Put']:
                raise ValueError("Side must be 'Call' or 'Put'")
            if exp == 'all':
                return self.options[self.options['type'] == side]
            else:
                return self.options[(self.options.expiry == exp) & (self.options['type'] == side)]
            
            
        def greeks(self, side, exp='all'):
            for row in self.get_side_exp(side, exp).itertuples():
                print('Strike = ', row.strike)
                print(self.option_info(row.instrument_name)['result']['greeks'])
                
                    
        
        @staticmethod
        def BS_CALL(S, K, T, r, sigma):
            N = stats.norm.cdf
            d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
            d2 = d1 - sigma * np.sqrt(T)
            return S * N(d1) - K * np.exp(-r*T)* N(d2)
        
        @staticmethod
        def BS_PUT(S, K, T, r, sigma):
            N = stats.norm.cdf
            d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
            d2 = d1 - sigma* np.sqrt(T)
            return K*np.exp(-r*T)*N(-d2) - S*N(-d1)
        
        def compare_price(self, side, exp='all'):
            if side == 'Call':
                calls = self.get_side_exp(side, exp)
                vols_a = []
                for row in calls.itertuples():
                    sigma = DeribitOptionsData.option_info(row.instrument_name)['result']['ask_iv'] / 100
                    V = DeribitOptionsData.BS_CALL(row.underlying_price, row.strike , row.time, 0, float(sigma))
                    vols_a.append(sigma)
                    print('#'*50)
                    print(f'Calculated BS {V}')
                    print(f'From api call {row.dollar_ask}')
                    print('STRIKE = ', row.strike)
                    print('Implied vol: ', sigma)
            elif side == 'Put':
                puts = self.get_side_exp(side, exp)
                vols_a = []
                for row in puts.itertuples():
                    sigma = DeribitOptionsData.option_info(row.instrument_name)['result']['ask_iv'] / 100
                    V = DeribitOptionsData.BS_PUT(row.underlying_price, row.strike , row.time, 0, float(sigma))
                    vols_a.append(sigma)
                    print('#'*50)
                    print(f'Calculated BS {V}')
                    print(f'From api call {row.dollar_ask}')
                    print('STRIKE = ', row.strike)
                    print('Implied vol: ', sigma)
                    
        def implied_vol_ask(self, side, exp='all'):
            if side == 'Call':
                calls = self.get_side_exp(side, exp)
                vols_a = []
                for row in calls.itertuples():
                    sigma = DeribitOptionsData.option_info(row.instrument_name)['result']['ask_iv'] / 100
                    V = DeribitOptionsData.BS_CALL(row.underlying_price, row.strike , row.time, 0, float(sigma))
                    vols_a.append(sigma)
                    calls['implied_vol_ask'] = vols_a
                    return calls
            elif side == 'Put':
                puts = self.get_side_exp(side, exp)
                vols_a = []
                for row in puts.itertuples():
                    sigma = DeribitOptionsData.option_info(row.instrument_name)['result']['ask_iv'] / 100
                    V = DeribitOptionsData.BS_PUT(row.underlying_price, row.strike , row.time, 0, float(sigma))
                    vols_a.append(sigma)
                    puts['implied_vol_ask'] = vols_a
                    return puts
                
        def changes(self, side, underlying_change, vol_change, exp='all'):
            if side == 'Call':
                calls = self.implied_vol_ask(side, exp)
                calls['expected_underlying_price'] = calls.underlying_price + underlying_change
                calls['expected_vol'] = calls.implied_vol_ask + vol_change
                calls['expected_option_price'] = DeribitOptionsData.BS_CALL(calls.expected_underlying_price, calls.strike, calls.time,calls.interest_rate, calls.expected_vol)
                return calls[['expected_underlying_price','expected_vol','strike', 'expected_option_price']]
            elif side == 'Put':
                puts = self.implied_vol_ask(side, exp)
                puts['expected_underlying_price'] = puts.underlying_price + underlying_change
                puts['expected_vol'] = puts.implied_vol_ask + vol_change
                puts['expected_option_price'] = DeribitOptionsData.BS_PUT(puts.expected_underlying_price, puts.strike, puts.time, puts.interest_rate, puts.expected_vol)
                return puts[['expected_underlying_price','expected_vol','strike', 'expected_option_price']]


# In[3]:

def DBDataGrabber(method, params):
    # method = Deribit function
    # params = params in dictionary format
    msg = {"method": method, "params": params}
    webdata = requests.get("https://www.deribit.com/api/v2/public/"+method,params)
    return webdata.json()


# In[4]:
#import datetime

def FutStaticDataGrab(eachccy):
    futstaticdatadict = {}
    futstaticdatadict[eachccy] = {}
    tempdata = DBDataGrabber("get_instruments",{"currency": eachccy, "kind": "future"})["result"]

    for eachfutno in tempdata:
        contractname = eachfutno["instrument_name"]
        clsdata = DBDataGrabber("get_last_settlements_by_instrument",{"instrument_name": contractname, "type": "settlement", "count":1})["result"]
        clsdatatemp = DBDataGrabber("get_last_settlements_by_instrument",{"instrument_name": eachccy+"-PERPETUAL", "type": "settlement", "count":1})["result"]
        
        futstaticdatadict[eachccy][contractname] = {}
        futstaticdatadict[eachccy][contractname]["Ccy"] = eachfutno["base_currency"]
        if contractname != eachccy+"-PERPETUAL":
            if clsdata["settlements"] != []:
                futstaticdatadict[eachccy][contractname]["FutType"] = "FUT"

                futstaticdatadict[eachccy][contractname]["EpochCls"] = round(clsdata["settlements"][0]["timestamp"],-2)
                futstaticdatadict[eachccy][contractname]["EpochExpiry"] = eachfutno["expiration_timestamp"]
                futstaticdatadict[eachccy][contractname]["PxCls"] = clsdata["settlements"][0]["mark_price"]
                futstaticdatadict[eachccy][contractname]["DateCls"] = dt.datetime.fromtimestamp(clsdata["settlements"][0]["timestamp"]/1000).astimezone(dt.timezone.utc).strftime('%d/%m/%Y %H:%M:%S')
                futstaticdatadict[eachccy][contractname]["DateExpiry"] = dt.datetime.fromtimestamp(eachfutno["expiration_timestamp"]/1000).astimezone(dt.timezone.utc).strftime('%d/%m/%Y %H:%M:%S')
                futstaticdatadict[eachccy][contractname]["DTECls"] = round((eachfutno["expiration_timestamp"] - clsdata["settlements"][0]["timestamp"])/(86400000),4)
            else:
                futstaticdatadict[eachccy][contractname]["FutType"] = "FUT"
                futstaticdatadict[eachccy][contractname]["EpochCls"] = round(clsdatatemp["settlements"][0]["timestamp"],-2)
                futstaticdatadict[eachccy][contractname]["EpochExpiry"] = eachfutno["expiration_timestamp"]
                futstaticdatadict[eachccy][contractname]["PxCls"] = 0
                futstaticdatadict[eachccy][contractname]["DateCls"] = dt.datetime.fromtimestamp(clsdatatemp["settlements"][0]["timestamp"]/1000).astimezone(dt.timezone.utc).strftime('%d/%m/%Y %H:%M:%S')
                futstaticdatadict[eachccy][contractname]["DateExpiry"] = dt.datetime.fromtimestamp(eachfutno["expiration_timestamp"]/1000).astimezone(dt.timezone.utc).strftime('%d/%m/%Y %H:%M:%S')
                futstaticdatadict[eachccy][contractname]["DTECls"] = round((eachfutno["expiration_timestamp"] - clsdatatemp["settlements"][0]["timestamp"])/(86400000),4)

        else:
            futstaticdatadict[eachccy][contractname]["FutType"] = "PERPETUAL"
            futstaticdatadict[eachccy][contractname]["EpochCls"] = round(clsdata["settlements"][0]["timestamp"],-2)
            futstaticdatadict[eachccy][contractname]["EpochExpiry"] = round(clsdata["settlements"][0]["timestamp"]+86400000,-2)-1 # to make sure its before first future
            futstaticdatadict[eachccy][contractname]["PxCls"] = clsdata["settlements"][0]["mark_price"]
            futstaticdatadict[eachccy][contractname]["DateCls"] = dt.datetime.fromtimestamp(clsdata["settlements"][0]["timestamp"]/1000).astimezone(dt.timezone.utc).strftime('%d/%m/%Y %H:%M:%S')
            futstaticdatadict[eachccy][contractname]["DateExpiry"] = dt.datetime.fromtimestamp((clsdata["settlements"][0]["timestamp"]+86400000)/1000).astimezone(dt.timezone.utc).strftime('%d/%m/%Y %H:%M:%S')
            futstaticdatadict[eachccy][contractname]["DTECls"] = round((clsdata["settlements"][0]["timestamp"]+86400000 - clsdata["settlements"][0]["timestamp"])/(86400000),4)

    # For SPOT, expiry epoch/date should be now time or timestamp, DTE = 0
    futstaticdatadict[eachccy][eachccy+"-SPOT"] = {}
    futstaticdatadict[eachccy][eachccy+"-SPOT"]["FutType"] = "SPOT"
    futstaticdatadict[eachccy][eachccy+"-SPOT"]["Ccy"] = eachccy
    futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochCls"] = round(futstaticdatadict[eachccy][eachccy+"-PERPETUAL"]["EpochCls"],-2)
    #futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochExpiry"] = futstaticdatadict[eachccy][eachccy+"-PERPETUAL"]["EpochExpiry"] - 1000
    futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochExpiry"] = futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochCls"]
    # For get_last_settlements_by_currency the delivery type gives the last close and settlement gives the close before the last close! If left as default, then can sometimes be "bankruptcy" type and the code fails
    futstaticdatadict[eachccy][eachccy+"-SPOT"]["PxCls"] =  DBDataGrabber("get_last_settlements_by_currency",{"currency": eachccy, "type":"delivery", "count":1})["result"]["settlements"][0]["index_price"]

    futstaticdatadict[eachccy][eachccy+"-SPOT"]["DateCls"] = dt.datetime.fromtimestamp(futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochCls"]/1000).strftime('%d/%m/%Y %H:%M:%S')
    #futstaticdatadict[eachccy][eachccy+"-SPOT"]["DateExpiry"] = datetime.datetime.fromtimestamp((clsdata["settlements"][0]["timestamp"]+86400000-1)/1000).astimezone(datetime.timezone.utc).strftime('%d/%m/%Y %H:%M:%S')
    futstaticdatadict[eachccy][eachccy+"-SPOT"]["DateExpiry"] = dt.datetime.fromtimestamp(futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochCls"]/1000).strftime('%d/%m/%Y %H:%M:%S')

    futstaticdatadict[eachccy][eachccy+"-SPOT"]["DTECls"] = 0 #round((futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochExpiry"] - futstaticdatadict[eachccy][eachccy+"-SPOT"]["EpochCls"])/(86400000),4)
    

    # mix in other function
    futtabledict = futstaticdatadict.copy()

    contractlist = futstaticdatadict[eachccy].keys()
    for contract in contractlist:
        if contract == eachccy+"-SPOT":
            futpxdata = DBDataGrabber("ticker",{"instrument_name":eachccy+"-PERPETUAL"})["result"]
            futstaticdatadict[eachccy][contract]["PxLive"] = futpxdata["index_price"]
            futstaticdatadict[eachccy][contract]["PxBid"] = 0
            futstaticdatadict[eachccy][contract]["PxAsk"] = 0
            futstaticdatadict[eachccy][contract]["PxChg"] = 0
            futstaticdatadict[eachccy][contract]["BidAskSprd"] = 0
            futstaticdatadict[eachccy][contract]["SprdTotLive"] = 0
            futstaticdatadict[eachccy][contract]["EpochFutTimestamp"] = futpxdata["timestamp"]
            futstaticdatadict[eachccy][contract]["DTELive"] = 0 
        else:
            futpxdata = DBDataGrabber("ticker",{"instrument_name":contract})["result"]
            futstaticdatadict[eachccy][contract]["PxLive"] = futpxdata["mark_price"]
            futstaticdatadict[eachccy][contract]["PxBid"] = futpxdata["best_bid_price"]
            futstaticdatadict[eachccy][contract]["PxAsk"] = futpxdata["best_ask_price"]
            if (len(clsdata["settlements"])>0):
              futstaticdatadict[eachccy][contract]["PxChg"] = round(futpxdata["mark_price"] - clsdata["settlements"][0]["mark_price"],4)
            else:
              futstaticdatadict[eachccy][contract]["PxChg"] = 0 
            futstaticdatadict[eachccy][contract]["BidAskSprd"] = round(futpxdata["best_bid_price"] - futpxdata["best_ask_price"],4)
            futstaticdatadict[eachccy][contract]["EpochFutTimestamp"] = futpxdata["timestamp"]
            futstaticdatadict[eachccy][contract]["DTELive"] = (futstaticdatadict[eachccy][contract]["EpochExpiry"] - futpxdata["timestamp"])/(86400000)
            futstaticdatadict[eachccy][contract]["SprdTotLive"] = round(futpxdata["mark_price"] - futpxdata["index_price"],4)
            futstaticdatadict[eachccy][contract]["SprdAnnLive"] = round(365.25*(futpxdata["mark_price"] - futpxdata["index_price"] / (futstaticdatadict[eachccy][contract]["EpochExpiry"] - futpxdata["timestamp"])/(86400000)),4)
            futstaticdatadict[eachccy][contract]["YldTotLive"] = round(futstaticdatadict[eachccy][contract]["SprdTotLive"]/futpxdata["index_price"],4)
            futstaticdatadict[eachccy][contract]["YldAnnLive"] = round(365.25*(futstaticdatadict[eachccy][contract]["YldTotLive"] / futstaticdatadict[eachccy][contract]["DTELive"] ),4)
            if (len(clsdata["settlements"])>0):
              futstaticdatadict[eachccy][contract]["SprdTotCls"] = round(futstaticdatadict[eachccy][contract]["PxCls"]-clsdata["settlements"][0]["mark_price"],4)
              futstaticdatadict[eachccy][contract]["YldTotCls"] = round(futstaticdatadict[eachccy][contract]["SprdTotCls"]/clsdata["settlements"][0]["mark_price"],4)
            else:
              futstaticdatadict[eachccy][contract]["SprdTotCls"] = 0
              futstaticdatadict[eachccy][contract]["YldTotCls"] = 0
            futstaticdatadict[eachccy][contract]["SprdAnnCls"] = round(365.25*(futstaticdatadict[eachccy][contract]["SprdTotCls"] / futstaticdatadict[eachccy][contractname]["DTECls"]),4)
            futstaticdatadict[eachccy][contract]["YldAnnCls"] = round(365.25*(futstaticdatadict[eachccy][contract]["YldTotCls"] / futstaticdatadict[eachccy][contract]["DTECls"]),4)
            futstaticdatadict[eachccy][contract]["YldAnnChg"] = round(futstaticdatadict[eachccy][contract]["YldAnnLive"] - futstaticdatadict[eachccy][contract]["YldAnnCls"],4)
            futstaticdatadict[eachccy][contract]["BidAskYld"] = futstaticdatadict[eachccy][contract]["BidAskSprd"]/futpxdata["index_price"]
       
    return futstaticdatadict[eachccy]
    

def FutLiveExtraCalc(eachccy, df):

    spotpxcls = df[(df["FutType"]=="SPOT")]["PxCls"].iloc[0]
    spotpxlive = df[(df["FutType"]=="SPOT")]["PxLive"].iloc[0]

    df["SprdTotCls"] = df["PxCls"]-spotpxcls
    df["SprdAnnCls"] = 365.25 * (df["SprdTotCls"][:-1] / df["DTECls"][:-1])
    df["YldTotCls"] = df["SprdTotCls"]/spotpxcls
    df["YldAnnCls"] = 365.25*(df["YldTotCls"][:-1] / df["DTECls"][:-1])

    df["SprdTotLive"] = df["PxLive"]-spotpxlive
    df["SprdAnnLive"] = 365.25*(df["SprdTotLive"][:-1] / df["DTELive"][:-1])
    df["YldTotLive"] = df["SprdTotLive"]/spotpxlive
    df["YldAnnLive"] = 365.25*(df["YldTotLive"][:-1] / df["DTELive"][:-1])

    df["SprdTotChg"] = df["SprdTotLive"] - df["SprdTotCls"]
    df["SprdAnnChg"] = df["SprdAnnLive"] - df["SprdAnnCls"]
    df["YldTotChg"] = df["YldTotLive"] - df["YldTotCls"]
    df["YldAnnChg"] = df["YldAnnLive"] - df["YldAnnCls"]

    df["BidAskSprd"] = df["PxAsk"]-df["PxBid"]
    df["BidAskYld"] = df["BidAskSprd"]/spotpxlive
    df["BidAskMarkPos"] = df[["PxLive","PxBid","PxAsk"]].apply(lambda row: max(row.PxLive-row.PxAsk,0) + min(row.PxLive-row.PxBid,0) if row.PxBid >0 and row.PxAsk >0 else "", axis=1)
    
    FutLiveTablesC = df[(df["FutType"]!="PERPETUAL")]
    
    df["DTEFwdCls"] = FutLiveTablesC["DTECls"]-FutLiveTablesC["DTECls"].shift(1)
    df["SprdFwdTotCls"] = FutLiveTablesC["SprdTotCls"]-FutLiveTablesC["SprdTotCls"].shift(1)
    
    df["SprdFwdAnnCls"] =  365.25*(df["SprdFwdTotCls"]/df["DTEFwdCls"])
    df["YldFwdTotCls"] = df["SprdFwdTotCls"]/spotpxcls
    df["YldFwdAnnCls"] = 365.25*(df["YldFwdTotCls"]/df["DTEFwdCls"])
    
    df["DTEFwdLive"] = FutLiveTablesC["DTELive"]-FutLiveTablesC["DTELive"].shift(1)
    df["SprdFwdTotLive"] = FutLiveTablesC["SprdTotLive"]-FutLiveTablesC["SprdTotLive"].shift(1)
    df["SprdFwdAnnLive"] =  365.25*(df["SprdFwdTotLive"]/df["DTEFwdLive"])
    df["YldFwdTotLive"] = df["SprdFwdTotLive"]/spotpxcls
    df["YldFwdAnnLive"] = 365.25*(df["YldFwdTotLive"]/df["DTEFwdLive"])
    
    df["SprdFwdTotChg"] = df["SprdFwdTotLive"] - df["SprdFwdTotCls"]
    df["SprdFwdAnnChg"] =  df["SprdFwdAnnLive"] - df["SprdFwdAnnCls"]
    df["YldFwdTotChg"] = df["YldFwdTotLive"] - df["YldFwdTotCls"]
    df["YldFwdAnnChg"] = df["YldFwdAnnLive"] - df["YldFwdAnnCls"]
    
    return df


# In[5]:

    
def generate_all_data():
    
    optObj = DeribitOptionsData('BTC')
    opts = optObj.options
    
    exp = optObj.expiries()
    list_of_maturities=[]
    for i in exp:
        list_of_maturities.append(i)
        
    strike=[]
    strike_list=[]
    open_int_c=[]
    open_int_p=[]
    call_cash=[]
    call_cash_total=[]
    put_cash=[]
    put_cash_total=[]
    total=[]
    max_pain_list=[]
    data=[]  
    
    
    # In[5]:
    
    df_maxpain_old = pd.read_csv("Max_Pain_data.csv") 
    df_maxpain_old.index = pd.to_datetime(df_maxpain_old.maturity_date)
    df_maxpain_old.max_pain = df_maxpain_old.max_pain.astype(float)
    df_maxpain_old = df_maxpain_old.iloc[:, 2:]
    
    def MaxPain():
        
        
        heading = ['maturity_date','max_pain']
        df_maxpain = pd.DataFrame(columns = heading)
        
        for i in range(len(exp)):
            calls = optObj.get_side_exp('Call', exp[i])
            puts = optObj.get_side_exp('Put', exp[i])
            a=calls[['strike', 'open_interest', 'time', 'expiry']].head(20)
            a=a.sort_values(by='strike')
            b=puts[['strike', 'open_interest', 'time', 'expiry']].head(20)
            b=b.sort_values(by='strike')
         
            for eachstrikepos in range(len(a["strike"])):
                strike.append(a["strike"].iloc[eachstrikepos])
                open_int_c.append(a["open_interest"].iloc[eachstrikepos])
            for eachstrikepos in range(len(b["strike"])):
                open_int_p.append(b["open_interest"].iloc[eachstrikepos])
            
            number_of_strikes=len(strike)
            
            for x in range(number_of_strikes):
                for y,z in zip(range(number_of_strikes),open_int_c):
                    if strike[x]-strike[y] >0:
                        call_cash.append((strike[x]-strike[y])*z)
                    else:
                        call_cash.append(0)
                call_cash_total.append(sum(call_cash))
                call_cash.clear()
            
            for x in range(number_of_strikes):
                for y,z in zip(range(number_of_strikes),open_int_p):
                    if strike[y]-strike[x] >0:
                        put_cash.append((strike[y]-strike[x])*z)
                    else:
                        put_cash.append(0)
                put_cash_total.append(sum(put_cash))
                put_cash.clear()
            
            for x,y in zip(call_cash_total,put_cash_total):
                total.append(x+y)
        
            
            min_position=total.index(min(total))
        
            
            if min(total)==0:
                max_pain='NaN'
            else:
                max_pain=strike[min_position]
            
                
            for x in range(len(strike)):
                if x==min_position-1:
                    max_pain_list.append('max_pain')
                else:
                    max_pain_list.append('-')
            
            for x in strike:
                strike_list.append(x)
                
            df_maxpain = df_maxpain.append(pd.DataFrame([[list_of_maturities[i],max_pain]], columns=list(heading)),ignore_index=True)
            
            total.clear()
            max_pain_list.clear()
            call_cash_total.clear()
            put_cash_total.clear()
            open_int_c.clear()
            strike.clear()
            open_int_p.clear()
            
        return df_maxpain
      
    df_maxpain = MaxPain()  
    
    df_maxpain.index = pd.to_datetime(df_maxpain.maturity_date)
    df_maxpain.max_pain = df_maxpain.max_pain.astype(float)
    df_maxpain = df_maxpain.iloc[:, 1:]
    df_maxpain.columns = df_maxpain.columns.astype(str)
    df_maxpain_old.columns = df_maxpain_old.columns.astype(str)
          
    df_maxpain_change = (df_maxpain - df_maxpain_old)\
        .combine_first(df_maxpain)\
        .combine_first(df_maxpain_old)\
        .fillna(float('NaN'))
        
    #display(df_maxpain_old)
    #display(df_maxpain)
    #display(df_maxpain_change)
    
    df_maxpain.to_csv("Max_Pain_data.csv")
    df_maxpain_change.to_csv("Max_Pain_Change_data.csv") 
    
    df_maxpain['maturity_date'] = df_maxpain.index.date
    df_maxpain = df_maxpain[['maturity_date', 'max_pain']]
    
    df_maxpain_change['maturity_date'] = df_maxpain_change.index.date
    df_maxpain_change = df_maxpain_change[['maturity_date', 'max_pain']]
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=df_maxpain.values, colLabels=df_maxpain.columns, loc='center')
    ax.set_title('Current Max Pain')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=df_maxpain_change.values, colLabels=df_maxpain_change.columns, loc='center')
    ax.set_title('Change in Max Pain')
    fig.tight_layout()
    plt.show()
    
    
    
    # In[7]:
    
    df_oi_old = pd.read_csv("OI_data.csv") 
    df_oi_old.index = pd.to_datetime(df_oi_old.maturity_date)
    df_oi_old = df_oi_old.iloc[:, 2:]
    df_oi_old = df_oi_old.astype(float)
    
    def OI():
    
        unique_strike = list(set(strike_list))
        unique_strike.sort()
        
        header = ['maturity_date']
        for i in unique_strike:
            header.append(i)
        df_oi = pd.DataFrame(columns = header)
        data_list=[]
        for i in range(len(exp)):
            data_list.append(list_of_maturities[i])
            call = optObj.get_side_exp('Call', exp[i])
            put = optObj.get_side_exp('Put', exp[i])
            a=call[['strike', 'open_interest', 'time','expiry']].head(20)
            a=a.sort_values(by='strike')
            b=put[['strike', 'open_interest', 'time','expiry']].head(20)
            b=b.sort_values(by='strike')
            
            for eachstrikepos in range(len(a["strike"])):
                strike.append(a["strike"].iloc[eachstrikepos])
                open_int_c.append(a["open_interest"].iloc[eachstrikepos])
            for eachstrikepos in range(len(b["strike"])):
                open_int_p.append(b["open_interest"].iloc[eachstrikepos])
            
            y=0
            for x in unique_strike:
                if x in strike: 
                    data_list.append(open_int_c[y]+open_int_p[y])
                    y=y+1
                else:
                    data_list.append(np.nan)
            
            df_oi = df_oi.append(pd.DataFrame([data_list], columns=list(header)),ignore_index=True)
        
            data_list.clear()
            strike.clear()
            open_int_c.clear()
            open_int_p.clear()
           
        return df_oi
    
    df_oi = OI()
    
    df_oi.index = pd.to_datetime(df_oi.maturity_date)
    df_oi = df_oi.iloc[:, 1:]
    df_oi = df_oi.astype(float)
    df_oi.columns = df_oi.columns.astype(str)
    df_oi_old.columns = df_oi_old.columns.astype(str)
            
    df_oi_change = (df_oi - df_oi_old)\
        .combine_first(df_oi)\
        .combine_first(df_oi_old)\
        .fillna(float('NaN'))
    
    #display(df_oi_old)
    #display(df_oi)
    #display(df_oi_change)
    
    df_oi.to_csv("OI_data.csv")
    df_oi_change.to_csv("OI_Change_data.csv")  
    
    cols = []
    idxs = []
    values = []
    
    for c in df_oi.columns:
        for v,i in zip(df_oi[c], df_oi.index):
            cols.append(c)
            idxs.append(i)
            values.append(v)
    
    top_10_oi = pd.DataFrame({'strikes': cols, 'maturities': idxs, 'oi': values})
    
    top_10_largest_oi = top_10_oi.nlargest(n=10, columns=['oi'], keep='first')
    top_10_smallest_oi = top_10_oi.nsmallest(n=10, columns=['oi'], keep='first')
    
    cols = []
    idxs = []
    values = []
    
    for c in df_oi_change.columns:
        for v,i in zip(df_oi_change[c], df_oi_change.index):
            cols.append(c)
            idxs.append(i)
            values.append(v)
    
    top_10_oi_change = pd.DataFrame({'strikes': cols, 'maturities': idxs, 'oi': values})
    
    top_10_largest_oi_change = top_10_oi_change.nlargest(n=10, columns=['oi'], keep='first')
    top_10_smallest_oi_change = top_10_oi_change.nsmallest(n=10, columns=['oi'], keep='first')
    
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_largest_oi.values, colLabels=top_10_largest_oi.columns, loc='center')
    ax.set_title('Top 10 Options with Highest OI')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_smallest_oi.values, colLabels=top_10_smallest_oi.columns, loc='center')
    ax.set_title('Top 10 Options with Lowest OI')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_largest_oi_change.values, colLabels=top_10_largest_oi_change.columns, loc='center')
    ax.set_title('Top 10 Options with Highest OI Change')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_smallest_oi_change.values, colLabels=top_10_smallest_oi_change.columns, loc='center')
    ax.set_title('Top 10 Options with Lowest OI Change')
    fig.tight_layout()
    plt.show()
    
    
    
    # In[9]:
    
    df_volume_old = pd.read_csv("Volume_data.csv") 
    df_volume_old.index = pd.to_datetime(df_volume_old.maturity_date)
    df_volume_old = df_volume_old.iloc[:, 2:]
    df_volume_old = df_volume_old.astype(float)
        
    def Volume():
    
        unique_strike = list(set(strike_list))
        unique_strike.sort()
        
        header = ['maturity_date']
        for i in unique_strike:
            header.append(i)
        df_volume = pd.DataFrame(columns = header)
        data_list=[]
        for i in range(len(exp)):
            data_list.append(list_of_maturities[i])
            call = optObj.get_side_exp('Call', exp[i])
            put = optObj.get_side_exp('Put', exp[i])
            a=call[['strike', 'volume', 'time', 'expiry']].head(20)
            a=a.sort_values(by='strike')
            b=put[['strike', 'volume', 'time', 'expiry']].head(20)
            b=b.sort_values(by='strike')
            
            for eachstrikepos in range(len(a["strike"])):
                strike.append(a["strike"].iloc[eachstrikepos])
                open_int_c.append(a["volume"].iloc[eachstrikepos])
            for eachstrikepos in range(len(b["strike"])):
                open_int_p.append(b["volume"].iloc[eachstrikepos])
            
            y=0
            for x in unique_strike:
                if x in strike: 
                    data_list.append(open_int_c[y]+open_int_p[y])
                    y=y+1
                else:
                    data_list.append(np.nan)
            
            df_volume = df_volume.append(pd.DataFrame([data_list], columns=list(header)),ignore_index=True)
            print(df_volume)
        
            data_list.clear()
            strike.clear()
            open_int_c.clear()
            open_int_p.clear()
       
        return df_volume
    
    df_volume = Volume()
    
    df_volume.index = pd.to_datetime(df_volume.maturity_date)
    df_volume = df_volume.iloc[:, 1:]
    df_volume = df_volume.astype(float)
    df_volume.columns = df_volume.columns.astype(str)
    df_volume_old.columns = df_volume_old.columns.astype(str)
            
    df_volume_change = (df_volume - df_volume_old)\
        .combine_first(df_volume)\
        .combine_first(df_volume_old)\
        .fillna(float('NaN'))
        
    #display(df_volume_old)
    #display(df_volume)
    #display(df_volume_change)
    
    
    df_volume.to_csv("Volume_data.csv")
    df_volume_change.to_csv("Volume_Change_data.csv")  
        
    cols = []
    idxs = []
    values = []
    
    for c in df_volume.columns:
        for v,i in zip(df_volume[c], df_volume.index):
            cols.append(c)
            idxs.append(i)
            values.append(v)
    
    top_10_volume = pd.DataFrame({'strikes': cols, 'maturities': idxs, 'volume': values})
    
    top_10_largest_volume = top_10_volume.nlargest(n=10, columns=['volume'], keep='first')
    top_10_smallest_volume = top_10_volume.nsmallest(n=10, columns=['volume'], keep='first')
    
    cols = []
    idxs = []
    values = []
    
    for c in df_volume_change.columns:
        for v,i in zip(df_volume_change[c], df_volume_change.index):
            cols.append(c)
            idxs.append(i)
            values.append(v)
    
    top_10_volume_change = pd.DataFrame({'strikes': cols, 'maturities': idxs, 'volume': values})
    
    top_10_largest_volume_change = top_10_volume_change.nlargest(n=10, columns=['volume'], keep='first')
    top_10_smallest_volume_change = top_10_volume_change.nsmallest(n=10, columns=['volume'], keep='first')
    
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_largest_volume.values, colLabels=top_10_largest_volume.columns, loc='center')
    ax.set_title('Top 10 Options with Highest Volume')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_smallest_volume.values, colLabels=top_10_smallest_volume.columns, loc='center')
    ax.set_title('Top 10 Options with Lowest Volume')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_largest_volume_change.values, colLabels=top_10_largest_volume_change.columns, loc='center')
    ax.set_title('Top 10 Options with Highest Volume Change')
    fig.tight_layout()
    plt.show()
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=top_10_smallest_volume_change.values, colLabels=top_10_smallest_volume_change.columns, loc='center')
    ax.set_title('Top 10 Options with Lowest Volume Change')
    fig.tight_layout()
    plt.show()
    
    
    
    # In[10]:
    
    
    '''
    index_prices = DBDataGrabber("get_delivery_prices", {"index_name":"btc_usd","count":1000})["result"]['data']
    index_prices = pd.DataFrame(index_prices)
    index_prices.head()
    index_prices.index = pd.to_datetime(index_prices.date)
    index_prices = index_prices.iloc[:, 0]
    
    plt.plot(index_prices)
    '''
    
    # In[10]:
    
    df_futures = FutStaticDataGrab("BTC")
    df_futures = pd.DataFrame(df_futures)
    df_futures = df_futures.T
    df_futures = FutLiveExtraCalc('BTC', df_futures)
    
    df_futures.to_csv('Fut_Table.csv')
    df_futures = pd.read_csv('Fut_Table.csv')
    
    df_futures = df_futures[['Unnamed: 0', 'SprdAnnCls', 'SprdAnnChg', 'YldAnnCls', 'YldAnnChg', 'YldFwdAnnCls', 'YldFwdAnnChg']]
    
    fig, ax = plt.subplots(1,1)
    fig.patch.set_visible(True)
    ax.axis('off')
    ax.axis('tight')
    ax.table(cellText=df_futures.values, colLabels=df_futures.columns, loc='center')
    ax.set_title('Futures Table')
    fig.tight_layout()
    plt.show()
    
    # In[11]:
    
    '''    
    from pytrends.request import TrendReq
    import time
    import gtrends
    
    pytrend = TrendReq(hl='en-US')
    keyword = 'bitcoin'
    start = '2020-01-01'
    end = '2022-06-24'
    geo='US'
    cat=0
    gprop=''
    
    from pytrends import dailydata
    
    start_d = datetime.datetime.strptime(start, '%Y-%m-%d')
    end_d = datetime.datetime.strptime(end, '%Y-%m-%d')
    s_year = start_d.year
    s_mon = start_d.month
    e_year = end_d.year
    e_mon = end_d.month
    
    df_gtrends = dailydata.get_daily_data(word= keyword,
                     start_year= s_year,
                     start_mon= s_mon,
                     stop_year= e_year,
                     stop_mon= e_mon,
                     geo= geo,
                     verbose= False,
                     wait_time = 1.0)
    
    df_gtrends['bitcoin'].plot(figsize=(15,10))
    df_gtrends['scale'].plot(figsize=(15,10))
    
    df_gtrends.to_csv('BTC_GoogleTrends.csv')
    df_gtrends = pd.read_csv('~/Quant Finance/OakTree & Lion/BTC_GoogleTrends.csv')    
    '''
    
    # In[12]:
    
    now = dt.datetime.now()
    otherday = now - dt.timedelta(days=365*2)
    
        
    df = DBDataGrabber("get_tradingview_chart_data", {"instrument_name":"BTC-PERPETUAL",
                                                      "start_timestamp": int(str(int(dt.datetime.timestamp(otherday))) + '000'), 
                                                      "end_timestamp": int(str(int(dt.datetime.timestamp(now))) + '000'),
                                                      "resolution": '1D'})["result"]
    
    df = pd.DataFrame(df)
    
    
    all_dates = pd.date_range(otherday, now, freq='1D', normalize=(True))
    
    if len(all_dates) != len(df.index):
        print('missing dates')
    else:
        df.index = all_dates
        
    df['returns'] = np.log(df['close']/df['close'].shift(1)).dropna()
    df = df[['open', 'high', 'low', 'close', 'returns']]
    
    # In[13]:
    
    def rolling_moments():
        dist_period = '30D'
        index_groups = df.resample(dist_period)
        index_groups = list(index_groups.groups)
        rolling = len(df.loc[index_groups[1]:index_groups[2]])
        
        changing_period = '1D'
        small_index_groups = df.resample(changing_period)
        small_index_groups = list(small_index_groups.groups)
        small_rolling = len(df.loc[small_index_groups[1]:small_index_groups[2]]) - 1
        
        rolling_dates = []
        rolling_price = []
        rolling_mean = []
        rolling_var = []
        rolling_skew = []
        rolling_kurt = []
        rolling_hyperskew = []
        rolling_hyperkurt = []
        
        for i in range(0, (len(small_index_groups)-int(str(dist_period)[:-1]))*small_rolling, small_rolling): 
            rolling_dates.append(str(df.iloc[i:rolling+i, -1].dropna().index[-1]))
            rolling_price.append(df.iloc[i:rolling+i, 3].dropna()[-1])
            rolling_mean.append(df.iloc[i:rolling+i, -1].dropna().mean())
            rolling_var.append(df.iloc[i:rolling+i, -1].dropna().var())
            rolling_skew.append(df.iloc[i:rolling+i, -1].dropna().skew())
            rolling_kurt.append(df.iloc[i:rolling+i, -1].dropna().kurt())
            rolling_hyperskew.append(stats.moment(df.iloc[i:rolling+i, -1].dropna(), moment = 5))
            rolling_hyperkurt.append(stats.moment(df.iloc[i:rolling+i, -1].dropna(), moment = 6))
        
            
        rolling_df = pd.DataFrame(list(zip(rolling_price, rolling_mean, rolling_var, rolling_skew, rolling_kurt, rolling_hyperskew, rolling_hyperkurt)), 
                                  index= pd.to_datetime(rolling_dates), 
                                  columns = ['price','mean', 'variance', 'skew', 'kurtosis', 'hyperskew', 'hyperkurt'])
        
        
        
        fig, ax = plt.subplots(1,1)
        ax.plot(rolling_df['price'])
        ax.set_title('Price')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(rolling_df['mean'])
        ax.set_title('Rolling Mean')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(rolling_df['variance']*365.25)
        ax.set_title('Rolling Variance')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(rolling_df['skew'])
        ax.set_title('Rolling Skew')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(rolling_df['kurtosis'])
        ax.set_title('Rolling Kurtosis')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(rolling_df['hyperskew'])
        ax.set_title('Rolling Hyper Skew')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(rolling_df['hyperkurt'])
        ax.set_title('Rolling Hyper Kurtosis')
        plt.show()
        
        return rolling_df
    
    rolling_df = rolling_moments()
    
    
    # In[14]:
    
    #Raw
    def vol_raw(price_data, window, trading_periods, clean):
        
        log_return = (price_data['close'] / price_data['close'].shift(1)).apply(np.log)
    
        result = log_return.rolling(
            window=window,
            center=False
        ).std() * math.sqrt(trading_periods)
    
        if clean:
            return result.dropna()
        else:
            return result
    
    #Garman Klass
    def vol_garman_klass(price_data, window, trading_periods, clean):
    
        log_hl = (price_data['high'] / price_data['low']).apply(np.log)
        log_co = (price_data['close'] / price_data['open']).apply(np.log)
    
        rs = 0.5 * log_hl**2 - (2*math.log(2)-1) * log_co**2
        
        def f(v):
            return (trading_periods * v.mean())**0.5
        
        result = rs.rolling(window=window, center=False).apply(func=f)
        
        if clean:
            return result.dropna()
        else:
            return result
    
    #Hodges Tompkins
    def vol_hedges_tompkins(price_data, window, trading_periods, clean):
        
        log_return = (price_data['close'] / price_data['close'].shift(1)).apply(np.log)
    
        vol = log_return.rolling(
            window=window,
            center=False
        ).std() * math.sqrt(trading_periods)
    
        h = window
        n = (log_return.count() - h) + 1
    
        adj_factor = 1.0 / (1.0 - (h / n) + ((h**2 - 1) / (3 * n**2)))
    
        result = vol * adj_factor
    
        if clean:
            return result.dropna()
        else:
            return result
        
    #Parkinson
    def vol_parkinson(price_data, window, trading_periods, clean):
    
        rs = (1.0 / (4.0 * math.log(2.0))) * ((price_data['high'] / price_data['low']).apply(np.log))**2.0
    
        def f(v):
            return (trading_periods * v.mean())**0.5
        
        result = rs.rolling(
            window=window,
            center=False
        ).apply(func=f)
        
        if clean:
            return result.dropna()
        else:
            return result
    
    #Rogers Satchell
    def vol_rogers_satchell(price_data, window, trading_periods, clean):
        
        log_ho = (price_data['high'] / price_data['open']).apply(np.log)
        log_lo = (price_data['low'] / price_data['open']).apply(np.log)
        log_co = (price_data['close'] / price_data['open']).apply(np.log)
        
        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
    
        def f(v):
            return (trading_periods * v.mean())**0.5
        
        result = rs.rolling(
            window=window,
            center=False
        ).apply(func=f)
        
        if clean:
            return result.dropna()
        else:
            return result
        
    #YangZhang    
    def vol_yang_zhang(price_data, window, trading_periods, clean):
    
        log_ho = (price_data['high'] / price_data['open']).apply(np.log)
        log_lo = (price_data['low'] / price_data['open']).apply(np.log)
        log_co = (price_data['close'] / price_data['open']).apply(np.log)
        
        log_oc = (price_data['open'] / price_data['close'].shift(1)).apply(np.log)
        log_oc_sq = log_oc**2
        
        log_cc = (price_data['close'] / price_data['close'].shift(1)).apply(np.log)
        log_cc_sq = log_cc**2
        
        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
        
        close_vol = log_cc_sq.rolling(
            window=window,
            center=False
        ).sum() * (1.0 / (window - 1.0))
        open_vol = log_oc_sq.rolling(
            window=window,
            center=False
        ).sum() * (1.0 / (window - 1.0))
        window_rs = rs.rolling(
            window=window,
            center=False
        ).sum() * (1.0 / (window - 1.0))
    
        k = 0.34 / (1.34 + (window + 1) / (window - 1))
        result = (open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * math.sqrt(trading_periods)
    
        if clean:
            return result.dropna()
        else:
            return result
    
    
    df['vol_raw'] = vol_raw(df, 7, 365.25, False)
    df['vol_garman_klass'] = vol_garman_klass(df, 7, 365.25, False)
    df['vol_hedges_tompkins'] = vol_hedges_tompkins(df, 7, 365.25, False)
    df['vol_parkinson'] = vol_parkinson(df, 7, 365.25, False)
    df['vol_rogers_satchell'] = vol_rogers_satchell(df, 24, 365.25, False)
    df['vol_yang_zhang'] = vol_yang_zhang(df, 7, 365.25, False)
    
    def plot_vols():
        
        '''
        plt.plot(df['vol_raw'], label = "Raw Vol")
        plt.plot(df["vol_garman_klass"], label = "Garman Klass")
        plt.plot(df["vol_hedges_tompkins"], label = "Hedges Tompkins")
        plt.title("Historical Volatility Measures", size = 10)
        plt.legend()
        plt.show()
        '''
        fig, ax = plt.subplots(1,1)
        ax.plot(df['vol_raw'])
        ax.set_title('vol_raw')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['vol_garman_klass'])
        ax.set_title('vol_garman_klass')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['vol_hedges_tompkins'])
        ax.set_title('vol_hedges_tompkins')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['vol_parkinson'])
        ax.set_title('vol_parkinson')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['vol_rogers_satchell'])
        ax.set_title('vol_rogers_satchell')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['vol_yang_zhang'])
        ax.set_title('vol_yang_zhang')
        plt.show()
        
        return
    
    plot_vols()
    
    
        
    def volatility():
    
        df['raw_vol_7'] = vol_raw(df, window=7, trading_periods=365.25, clean=False)
        df['raw_vol_28'] = vol_raw(df, window=28, trading_periods=365.25, clean=False)
        df['raw_vol_60'] = vol_raw(df, window=60, trading_periods=365.25, clean=False)
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['raw_vol_7'])
        ax.set_title('Realized Volatility - 7 Days Rolling')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['raw_vol_28'])
        ax.set_title('Realized Volatility - 28 Days Rolling')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['raw_vol_60'])
        ax.set_title('Realized Volatility - 60 Days Rolling')
        plt.show()
        
        df['raw_vol_change_7'] = df['raw_vol_7'] - df['raw_vol_7'].shift(7)
        df['raw_vol_change_7'].dropna()[:7]
        
        df['raw_vol_change_28'] = df['raw_vol_28'] - df['raw_vol_28'].shift(7)
        df['raw_vol_change_28'].dropna()[:7]
        
        df['raw_vol_change_60'] = df['raw_vol_60'] - df['raw_vol_60'].shift(7)
        df['raw_vol_change_60'].dropna()[:7]
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['raw_vol_change_7'])
        ax.set_title('Realized Volatility - 7 Days Rolling Change')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['raw_vol_change_28'])
        ax.set_title('Realized Volatility - 28 Days Rolling Change')
        plt.show()
        
        fig, ax = plt.subplots(1,1)
        ax.plot(df['raw_vol_change_60'])
        ax.set_title('Realized Volatility - 60 Days Rolling Change')
        plt.show()
        
        return df 
    
    df = volatility()
    
    
    # In[15]:
    
        
    df = df.iloc[:, 0:5]
    
    #Var
    def vol_var(price_data, window, clean):
    
        log_return = (price_data['close'] / price_data['close'].shift(1)).apply(np.log)
    
        result = log_return.rolling(
            window=window,
            center=False
        ).var()
    
        if clean:
            return result.dropna()
        else:
            return result
        
    df['vol_var'] = vol_var(df, 7, False)
    df['vol_garman_klass'] = vol_garman_klass(df, 7, 365.25, False)
    
    df = df.dropna()
    
    train = df.loc[:"2022-1-1"]
    test = df.loc["2022-1-2":]
    
    #plt.plot(train['close'])
    #plt.plot(test['close'])
    
    #Hurst Function
    #Value near 0.5 indicates a random series.
    #Value near 0 indicates a mean reverting series.
    #Value near 1 indicates a trending series.
    
    def hurst(ts):
        """Returns the Hurst Exponent of the time series vector ts"""
        # Create the range of lag values
        lags = range(2, 100)
        # Calculate the array of the variances of the lagged differences
        tau = [sqrt(std(subtract(ts[lag:], ts[:-lag]))) for lag in lags] 
        # Use a linear fit to estimate the Hurst Exponent
        poly = polyfit(log(lags), log(tau), 1)
        # Return the Hurst exponent from the polyfit output
        return poly[0]*2.0
    
    hurst(ts = df)
    
        
    # Plot ACF, PACF and Q-Q plot and get ADF p-value of series
        
    def plot_correlogram(x, lags=None, title=None):    
        lags = min(10, int(len(x)/5)) if lags is None else lags
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 8))
        x.plot(ax=axes[0][0])
        q_p = np.max(q_stat(acf(x, nlags=lags), len(x))[1])
        #stats = f'Q-Stat: {np.max(q_p):>8.2f}\nADF: {adfuller(x)[1]:>11.2f} \nHurst: {round(hurst(x.values),2)}'
        #axes[0][0].text(x=.02, y=.85, s=stats, transform=axes[0][0].transAxes)
        probplot(x, plot=axes[0][1])
        mean, var, skew, kurtosis = moment(x, moment=[1, 2, 3, 4])
        s = f'Mean: {mean}\nVariance: {var}\nSkew: {skew}\nKurtosis:{kurtosis}'
        axes[0][1].text(x=.02, y=.75, s=s, transform=axes[0][1].transAxes)
        plot_acf(x=x, lags=lags, zero=False, ax=axes[1][0])
        plot_pacf(x, lags=lags, zero=False, ax=axes[1][1])
        axes[1][0].set_xlabel('Lag')
        axes[1][1].set_xlabel('Lag')
        fig.suptitle(title, fontsize=20)
        fig.tight_layout()
        fig.subplots_adjust(top=.9)
        return
        
    plot_correlogram(df['returns'], lags=100, title='BTC (Log, Diff)')
    
    #CREATE GARCH
    garch_df = pd.DataFrame(df["returns"].shift(1).loc[df.index])
    garch_df.at[train.index, "returns"] = train["returns"]
    basic_gm = arch_model(100 * garch_df['returns'], p = 1, q = 1, 
                          mean = 'constant', vol = 'GARCH', dist = 'normal')
    
    # Fit the model
    gm_result = basic_gm.fit(last_obs = test.index[0], update_freq = 5)
    print(gm_result.summary())
    gm_result.plot()
    plt.show()
    
    # Get model conditional volatility
    gm_vol = gm_result.conditional_volatility
    plt.plot(gm_vol, color = 'blue', label = 'Basic Garch Volatility')
    plt.plot(100* df['returns'], color = 'grey', 
             label = 'Daily Returns', alpha = 0.4)
    plt.legend(loc = 'upper right')
    plt.show()
    
    #Forecasting
    
    #Check Predictions - conditional variance
    gm_pred = test.copy()
    gm_pred["variance_prediction"] = gm_result.forecast().variance.loc[test.index]
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(gm_pred["variance_prediction"]/100, label = "Garch Variance Prediction")
    #plt.plot(gm_pred["vol_var"]*100, label = "Rolling Variance",  alpha=0.4)
    plt.plot(gm_pred["returns"], label = "Returns",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    #Check Predictions - conditional variance
    gm_pred["variance_prediction"] = gm_result.forecast().variance.loc[test.index]
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(gm_pred["variance_prediction"]/100, label = "Garch Variance Prediction")
    plt.plot(gm_pred["vol_var"]*100, label = "Rolling Variance",  alpha=0.4)
    #plt.plot(gm_pred["returns"], label = "Returns",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    #Check Predictions - conditional variance
    gm_pred["variance_prediction"] = gm_result.forecast().variance.loc[test.index]
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(gm_pred["variance_prediction"], label = "Garch Variance Prediction")
    plt.plot(test["vol_garman_klass"]*10, label = "Garman Klass",  alpha=0.4)
    #plt.plot(gm_pred["returns"], label = "Returns",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    
    # Make forecast
    gm_forecast = gm_result.forecast(horizon = 5)
    print(gm_forecast.residual_variance[-1:]/100)
    gm_var_forecast = gm_forecast.residual_variance[-1:].transpose()
    plt.plot(gm_var_forecast/100)
    
    # Obtain model estimated residuals and volatility
    gm_resid = gm_result.resid
    gm_std = gm_result.conditional_volatility
    # Calculate the standardized residuals
    gm_std_resid = gm_resid/gm_std
    # Plot the histogram of the standardized residuals
    plt.figure(figsize=(7,4))
    sns.distplot(gm_std_resid, norm_hist=True, fit=stats.norm, bins=50, color='r')
    plt.legend(('normal', 'standardized residuals'))
    plt.show()
    
    print(gm_std_resid.mean())
    print(gm_std_resid.var())
    print(gm_std_resid.skew())
    print(gm_std_resid.kurt())
    
    
    # In[15]:
    
    
    '''
    #Optuna
    import optuna
    from optuna.visualization import plot_edf
    from optuna.visualization import plot_intermediate_values
    from optuna.visualization import plot_optimization_history
    from optuna.visualization import plot_param_importances
    
    #Optimization
    def objective(trial):
        
         
          y = trial.suggest_categorical('y', [df['returns']])
          
          mean = trial.suggest_categorical('mean', ['constant', 'zero'])
          lags = trial.suggest_int('lags', 1, 100)
          vol = trial.suggest_categorical('vol', ['GARCH', 'FIGARCH', 'EGARCH',
                                                  'HARCH', 'ARCH'])
          p = trial.suggest_int('p', 1, 1)
          q = trial.suggest_int('q', 0, 1)
          dist = trial.suggest_categorical('dist', ['normal', 'studentst', 
                                                    'skewstudent'])
          
          opt_arch = arch_model(y=100 * y, 
                                mean=mean,
                                lags=lags,
                                vol=vol, 
                                p=p, 
                                q=q, 
                                dist=dist) 
          
          train = df.loc[:"2022-1-1"],
          test = df.loc["2022-1-2":]
          
          #Return most accurate model within train data
          results = opt_arch.fit(last_obs = test.index[0], update_freq = 5)
          score = results.bic
          return score
      
    
    study = optuna.create_study(direction = "minimize", 
                                sampler = optuna.samplers.TPESampler())
    
    
    #Tree-structured Parzen Estimator algorithm implemented in optuna.samplers.TPESampler
    #CMA-ES based algorithm implemented in optuna.samplers.CmaEsSampler
    #Grid Search implemented in optuna.samplers.GridSampler
    #Random Search implemented in optuna.samplers.RandomSampler
    
    study.optimize(objective, n_trials = 200, timeout = 120)
    
    #Check and visualize scores of Optimization on Train dataset
    print(study.best_value)
    print(study.best_params)
    print(study.best_trial)
    #optuna.visualization.plot_optimization_history(study).show()
    #optuna.visualization.plot_param_importances(study).show()
    #optuna.visualization.plot_edf(study).show()
    '''
    
    
    # In[15]:
    
    
    #CREATE OPTIMIZED ARCH
    optarch_df = pd.DataFrame(df["returns"].shift(1).loc[df.index])
    optarch_df.at[train.index, "returns"] = train["returns"]
    optarch = arch_model(100 * optarch_df['returns'], p = 1, q = 1, 
                          mean = 'constant', lags=74, vol = 'EGARCH', dist = 'studentst')
    
    # Fit the model
    optarch_result = optarch.fit(last_obs = test.index[0], update_freq=5)
    print(optarch_result.summary())
    optarch_result.plot()
    plt.show()
    
    # Get model conditional volatility
    optarch_vol = optarch_result.conditional_volatility
    plt.plot(optarch_vol, color = 'red', label = 'Optimized Conditional Volatility')
    plt.plot(100 * df['returns'], color = 'grey', label = 'Daily Returns', alpha = 0.4)
    plt.legend(loc = 'upper right')
    plt.show()
    
    #Check Predictions - conditional variance
    optarch_pred = test.copy()
    optarch_pred["variance_prediction"] = optarch_result.forecast().variance.loc[test.index]
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(optarch_pred["variance_prediction"]/100, label = "Optimized Variance Prediction")
    #plt.plot(optarch_pred["vol_var"]*100, label = "Rolling Variance",  alpha=0.4)
    plt.plot(optarch_pred["returns"], label = "Returns",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    #Check Predictions - conditional variance
    optarch_pred["variance_prediction"] = optarch_result.forecast().variance.loc[test.index]
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(optarch_pred["variance_prediction"]/100, label = "Optimized Variance Prediction")
    plt.plot(optarch_pred["vol_var"]*100, label = "Rolling Variance",  alpha=0.4)
    #plt.plot(optarch_pred["returns"], label = "Actuals",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    
    #Check Predictions - conditional variance
    optarch_pred["variance_prediction"] = optarch_result.forecast().variance.loc[test.index]
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(optarch_pred["variance_prediction"], label = "Optimized Variance Prediction")
    plt.plot(test["vol_garman_klass"]*10, label = "Garman Klass",  alpha=0.4)
    #plt.plot(optarch_pred["returns"], label = "Actuals",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    # Make forecast
    optarch_forecast = optarch_result.forecast(horizon = 5, method='simulation')
    print(optarch_forecast.residual_variance[-1:]/100)
    optarch_var_forecast = optarch_forecast.residual_variance[-1:].transpose()
    plt.plot(optarch_var_forecast/100)
    
    #COMPARE MODELS
    # Print each models BIC
    print(f'BASIC GARCH BIC: {gm_result.bic}')
    print(f'\nOPTIMIZED ARCH BIC: {optarch_result.bic}')
    
    # Print each models AIC
    print(f'BASIC GARCH AIC: {gm_result.aic}')
    print(f'\nOPTIMIZED ARCH AIC: {optarch_result.aic}')
    
    
    # Obtain model estimated residuals and volatility
    optarch_resid = optarch_result.resid
    optarch_std = optarch_result.conditional_volatility
    # Calculate the standardized residuals
    optarch_std_resid = optarch_resid/optarch_std
    # Plot the histogram of the standardized residuals
    plt.figure(figsize=(7,4))
    sns.distplot(optarch_std_resid, norm_hist=True, fit=stats.norm, bins=50, color='r')
    plt.legend(('normal', 'standardized residuals'))
    plt.show()
    
    print(optarch_std_resid.mean())
    print(optarch_std_resid.var())
    print(optarch_std_resid.skew())
    print(optarch_std_resid.kurt())
    
    
    
    #COMBINED PLOTS
    #Check Fitting - conditional variance
    plt.plot(optarch_vol, color = 'red', label = 'Optimized Conditional Volatility')
    plt.plot(gm_vol, color = 'blue', label = 'Garch Conditional Volatility')
    plt.plot(100 * df['returns'], color = 'grey', label = 'Daily Returns', alpha = 0.4)
    plt.legend(loc = 'upper right')
    plt.show()
    
    #Check Predictions - conditional variance
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(optarch_pred["variance_prediction"]/100, label = "Optimized Variance Prediction")
    plt.plot(gm_pred["variance_prediction"]/100, label = "Garch Variance Prediction")
    plt.plot(optarch_pred["vol_var"]*100, label = "Rolling Variance",  alpha=0.4)
    plt.plot(test["vol_garman_klass"]/10, label = "Garman Klass",  alpha=0.4)
    plt.plot(optarch_pred["returns"], label = "Actuals",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    
    
    # In[15]:
    
    
    '''How to go about forecasting in the long run?'''
    #EXPANDING VS FIXED FORECAST
    
    #FIXED: creates forecasts throughout time looking at all of previous data
    #EXPANDING: creates forecasts while dropping old data points hile passing through time
    
    
    #expanding
    end = []
    forecasts = []
    for i in range(len(train), len(train+test)-1):
        sys.stdout.write('-')
        sys.stdout.flush()
        res = optarch.fit(first_obs = df.index[0], last_obs = df.index[i], disp='off')
        forc = res.forecast(horizon=1, method='simulation').variance.loc[df.index[i+1]]
        end.append(df.index[i+1])
        forecasts.append(forc.iloc[0])
        print(str(i) + '/' + str(len(train+test)))
    
    variance_expanding = pd.DataFrame(forecasts, index = end, columns=['expanding_variance_forecast'])
    
    #rolling
    window = 365
    end = []
    forecasts = []
    for i in range(len(train), len(train+test)-1):
        sys.stdout.write('-')
        sys.stdout.flush()
        res = optarch.fit(first_obs = df.index[i-365], last_obs = df.index[i], disp='off')
        forc = res.forecast(horizon=1, method='simulation').variance.loc[df.index[i+1]]
        end.append(df.index[i+1])
        forecasts.append(forc.iloc[0])
        print(str(i) + '/' + str(len(train+test)))
    
    variance_rolling = pd.DataFrame(forecasts, index = end, columns=['rolling_variance_forecast'])
    
    arch_backtest = test
    arch_backtest['expanding_variance_forecast'] = variance_expanding["expanding_variance_forecast"]
    arch_backtest['rolling_variance_forecast'] = variance_rolling["rolling_variance_forecast"]
    arch_backtest['normal_variance_forecast'] = optarch_pred["variance_prediction"]
    
    
    plt.rcParams["figure.figsize"] = 18, 5
    plt.plot(arch_backtest["expanding_variance_forecast"]/100, label = "Expanding Variance Forecast")
    plt.plot(arch_backtest["rolling_variance_forecast"]/100, label = "Rolling Variance Forecast")
    plt.plot(arch_backtest["normal_variance_forecast"]/100, label = "Normal Variance Forecast")
    plt.plot(arch_backtest["vol_garman_klass"]*25, label = "Garman_Klass",  alpha=0.4)
    plt.plot(arch_backtest["vol_var"]*100, label = "Rolling Variance",  alpha=0.4)
    plt.plot(arch_backtest["returns"], label = "Returns",  alpha=0.4)
    plt.title("Actuals vs Predictions for BTC Variance", size = 24)
    plt.legend()
    plt.show()
    
    
    
    # In[16]:
    
    
    vol_high_low = []
    
    for current, future in zip(arch_backtest['vol_raw'], arch_backtest['vol_raw'].shift(-1)):
        print(current, future)
        if future > current:
            vol_high_low.append(1)
        else:
            vol_high_low.append(0)
            
    arch_backtest['vol_hl'] = vol_high_low
    
    forecast_high_low = []
    
    for current, future in zip(arch_backtest['expanding_variance_forecast'], arch_backtest['expanding_variance_forecast'].shift(-1)):
        if future > current:
            forecast_high_low.append(1)
        else:
            forecast_high_low.append(0)
    
    
    arch_backtest['for_hl'] = forecast_high_low
    
    accuracy = []
    
    for current, forecast in zip(arch_backtest['vol_hl'], arch_backtest['for_hl']):
        if forecast == current:
            accuracy.append(1)
        else:
            accuracy.append(0)
    
    
    arch_backtest['accuracy'] = accuracy
    
    arch_backtest['accuracy'].value_counts(normalize=True)
    len(arch_backtest[arch_backtest['accuracy'] == 1])
    len(arch_backtest[arch_backtest['accuracy'] == 0])
    
    
    
    arch_backtest['vol_%diff'] = arch_backtest['vol_garman_klass'].pct_change() * 100
    arch_backtest['for_%diff'] = arch_backtest['expanding_variance_forecast'].pct_change() * 100
    
    plt.plot(arch_backtest['vol_%diff'], label='Vol % Diff')
    plt.plot(arch_backtest['for_%diff'], label='Forecast % Diff')
    plt.title("Actual vs Predicted Change in Vol", size = 24)
    plt.legend()
    plt.show()
    
    
    MSE = ((arch_backtest['vol_%diff'] - arch_backtest['for_%diff'])**2).mean()
    RMSE = np.sqrt(((arch_backtest['vol_%diff'] - arch_backtest['for_%diff'])**2).mean())
    
    return

generate_all_data()

# In[16]:



PLOT_DIR = 'O&L_Historical_Analysis_Reports_'
PLOT_DIR = PLOT_DIR + str(dt.datetime.date(dt.datetime.now()))

def construct():
    # Delete folder if exists and create it again
    try:
        shutil.rmtree(PLOT_DIR)
        os.mkdir(PLOT_DIR)
    except FileNotFoundError:
        os.mkdir(PLOT_DIR)
        
    
    # Construct data shown in document
    counter = 0
    pages_data = []
    temp = []
    # Get all plots
    files = os.listdir(PLOT_DIR)
    # Sort them by month - a bit tricky because the file names are strings
    files = sorted(os.listdir(PLOT_DIR), key=lambda x: int(x.split('.')[0]))
    # Iterate over all created visualization
    for fname in files:
        # We want 3 per page
        if counter == 3:
            pages_data.append(temp)
            temp = []
            counter = 0

        temp.append(f'{PLOT_DIR}/{fname}')
        counter += 1

    return [*pages_data, temp]



plots_per_page = construct()
plots_per_page



#for all plots just add 'filename' and send them to the folder. Once in there, 
#the folder is easy to set as a pdf