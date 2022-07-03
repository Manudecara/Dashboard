from datetime import datetime
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from scipy.stats import skew, kurtosis, zscore, norm
import scipy.stats as stats
import math
import streamlit as st
import glob
import seaborn as sns

import plotly.figure_factory as ff
from dateutil.relativedelta import relativedelta # to add days or years
import time
from time import sleep
from datetime import date

import cmasher as cmr
import scipy.stats

from bokeh.plotting import figure
from bokeh.models import Legend

import base64
import collections
import asyncio
import nest_asyncio
import csv
import os

from tardis_client import TardisClient, Channel
from tardis_dev import datasets, get_exchange_details
from helper_functions import distr_selectbox_names,creating_dictionaries

#nest_asyncio.apply()
#pio.renderers.default = 'svg'
pio.renderers.default = 'browser'
plot_mode = 'Dark Mode'

os.chdir("/Users/manu/Quant Finance/OakTree & Lion")
#logging.basicConfig(level=logging.DEBUG)
#st.set_option('deprecation.showPyplotGlobalUse', False)
   
#df = pd.read_csv('/Users/manu/Quant Finance/OakTree & Lion/BTC-PERPETUAL_1M.csv')
#df = pd.read_csv('/Users/manu/Quant Finance/OakTree & Lion/Kucoin_BTCUSDT_2022_minute.csv', skiprows=1)


def page_load_and_fit():
    
    df_skiprows = st.checkbox('Skip rows?')

    file = st.file_uploader("Upload File", type={"csv", "txt"})
        
    if file is not None:
        if df_skiprows:
            df = pd.read_csv(file, skiprows=1)
        else:
            df = pd.read_csv(file)
        
        st.write(df)
        
        find_index = st.selectbox("Select index variable",
                                  options = list(df.columns),
                                  format_func=lambda x: x)
        
        #find_index = 'date'
        df.index = df[find_index]
        df.index = pd.to_datetime(df.index)
        
        find_price = st.selectbox("Select price variable",
                                   options = list(df.columns),
                                   format_func=lambda x: x)
         
        #find_price = 'close'
        df = df[[find_price]]
        df[find_price] = df[find_price].astype(float)
        
        st.write(df)
        
        df_reindex = st.checkbox('Re-index?')
        if df_reindex:
            df = df.reindex(index=df.index[::-1])
        
        date_start = datetime.timestamp(pd.to_datetime(df.index[0]))
        date_start = datetime.fromtimestamp(date_start)
        date_end = datetime.timestamp(pd.to_datetime(df.index[-1]))
        date_end = datetime.fromtimestamp(date_end)
        
        all_dates = pd.date_range(date_start, date_end, freq='1min')
        missing_dates = pd.date_range(date_start, date_end, freq='1min').difference(df.index)
        
        st.write(str(len(missing_dates)) + ' missing rows, fill with next non-missing value?')
        
        df_fill = st.checkbox('Re-fill?')
        if df_fill:
            df = df[~df.index.duplicated()]
            df = df.reindex(all_dates, fill_value=np.nan)
            df['close'] = df['close'].ffill()
        
        
        choose_returns = st.selectbox("Select returns to calculate",
                                      options = list(['daily_log', 'hourly_log', 'minute_log']),
                                      format_func=lambda x: x)
        
        if choose_returns == 'daily_log':
            df['returns'] = np.log(df['close']/df['close'].shift(1)).dropna()
        
        elif choose_returns == 'hourly_log':
            df['returns'] = ((1+df['returns'])**(24))-1
            
        elif choose_returns == 'minute_log':
            df['returns'] = ((1+df['returns'])**(60*24))-1
        
        #df['returns'] = df[find_price].pct_change().fillna(method='bfill')*100
        
        df.columns = ['price', 'returns']

        st.title("General data information:")
        st.text("---  Data has " + str(len(df)) + " rows of data.")
        st.write(df)
        

        format = 'YY/MM/DD - hh:mm:ss'  
        date_start = datetime.timestamp(pd.to_datetime(df.index[0]))
        date_start = datetime.fromtimestamp(date_start)
        date_end = datetime.timestamp(pd.to_datetime(df.index[-1]))
        date_end = datetime.fromtimestamp(date_end)
        
        #missing_dates = pd.date_range(date_start, date_end, freq='1min').difference(df.index)

        slider = st.slider('Select date', min_value=date_start, value=(date_start, date_end), max_value=date_end , format=format)
        st.write("Start time:", slider[0])
        st.write("End time:", slider[1])

        df = df.loc[slider[0]:slider[1]]
        #df = df.loc['2019-01-01 00:01:00':'2022-03-1 00:00:00']

        
        #choose 1 of the 4
        rolling_analysis = st.checkbox('Rolling anlysis')
        group_analysis = st.checkbox('Group analysis')
        reconstruction_analysis = st.checkbox('Reconstruction analysis')
        whole_analysis = st.checkbox('Whole analysis')
        
        if rolling_analysis:
            
            dist_period = st.text_input('State distribution period', '30D')
            index_groups = df.resample(dist_period)
            index_groups = list(index_groups.groups)
            rolling = len(df.loc[index_groups[1]:index_groups[2]])
            st.write('Every distribution of: ' + dist_period + ', will have: ' + str(rolling) + ' data points')
            
            changing_period = st.text_input('State distribution change period', '1D')
            small_index_groups = df.resample(changing_period)
            small_index_groups = list(small_index_groups.groups)
            small_rolling = len(df.loc[small_index_groups[1]:small_index_groups[2]]) - 1
            
            if changing_period == '1Min':
            
                rolling_df = pd.DataFrame(data = df['returns'].rolling(rolling).mean(), index = df.index)
                rolling_df['mean'] = rolling_df.iloc[:, 0]
                rolling_df['variance'] = df['returns'].rolling(rolling).var()
                rolling_df['skew'] = df['returns'].rolling(rolling).skew()
                rolling_df['kurtosis'] = df['returns'].rolling(rolling).kurt()
                rolling_df = rolling_df[['mean','variance','skew','kurtosis']]
                rolling_df.index = pd.to_datetime(rolling_df.index)

            else:
            
                rolling_dates = []
                rolling_price = []
                rolling_mean = []
                rolling_var = []
                rolling_skew = []
                rolling_kurt = []
                rolling_hyperskew = []
                rolling_hyperkurt = []
    
                for i in range(0, (len(small_index_groups)-int(str(dist_period)[:-1]))*small_rolling, small_rolling): 
                    rolling_dates.append(str(df.iloc[i:rolling+i, 1].dropna().index[-1]))
                    rolling_price.append(df.iloc[i:rolling+i, 0].dropna()[-1])
                    rolling_mean.append(df.iloc[i:rolling+i, 1].dropna().mean())
                    rolling_var.append(df.iloc[i:rolling+i, 1].dropna().var())
                    rolling_skew.append(df.iloc[i:rolling+i, 1].dropna().skew())
                    rolling_kurt.append(df.iloc[i:rolling+i, 1].dropna().kurt())
                    rolling_hyperskew.append(scipy.stats.moment(df.iloc[i:rolling+i, 1].dropna(), moment = 5))
                    rolling_hyperkurt.append(scipy.stats.moment(df.iloc[i:rolling+i, 1].dropna(), moment = 6))

                    
                rolling_df = pd.DataFrame(list(zip(rolling_price, rolling_mean, rolling_var, rolling_skew, rolling_kurt, rolling_hyperskew, rolling_hyperkurt)), 
                                          index= pd.to_datetime(rolling_dates), 
                                          columns = ['price','mean','variance','skew','kurtosis', 'hyperskew', 'hyperkurt'])
                
            rolling_distributions = st.checkbox('Plot rolling distributions')
    
            if rolling_distributions:
                
                for i in range(0, (len(small_index_groups)-int(str(dist_period)[:-1]))*small_rolling, small_rolling): 
                
                    start = str(df.iloc[i:rolling+i, 1].dropna().index[0])
                    end = str(df.iloc[i:rolling+i, 1].dropna().index[-1])
                    data = zscore(df.iloc[i:rolling+i, 1].dropna())
                    
                    fig, ax = plt.subplots(1,1)
                    
                    mu = 0
                    sigma = 1
                    ax.hist(data, bins='auto', density = True, color = 'grey')
                    
                    pdf_plot = np.linspace(min(data), 
                                         max(data), 
                                         len(data))
                    
                    ax.plot(pdf_plot, norm.pdf(pdf_plot), linewidth=1, color='yellow', 
                            label=f'Start: {start}\nEnd: {end}\nMean: {round(data.mean(),5)}\nVariance: {round(data.var(),5)}\nSkew: {round(skew(data),3)}\nKurtosis: {round(kurtosis(data),3)}')
                    
                    median, q1, q3 = np.percentile(data, 50), np.percentile(data, 25), np.percentile(data, 75)
                                    
                    ax.vlines(median, ymin= 0, ymax= 0.8, color='r', linestyle='--', linewidth=0.5)                
                    ax.vlines(q1, ymin= 0, ymax= 0.8, color='b', linestyle='--', linewidth=0.5)                
                    ax.vlines(q3, ymin= 0, ymax= 0.8, color='b', linestyle='--', linewidth=0.5)            
                    
                    ax.annotate("{:.1f}%".format(100*norm(mu, sigma).cdf(q1)), xy=((q1-1.5*(q3-q1)+q1)/2, 0), ha='right', color='white')
                    ax.annotate("{:.1f}%".format(100*(norm(mu, sigma).cdf(q3)-norm(mu, sigma).cdf(q1))), xy=(median, 0), ha='center', color='white')
                    ax.annotate("{:.1f}%".format(100*(norm(mu, sigma).cdf(q3+1.5*(q3-q1)-q3)-norm(mu, sigma).cdf(q3))), xy=((q3+1.5*(q3-q1)+q3)/2, 0), ha='left', color='white')
                    ax.annotate('q1', xy=(q1, norm(mu, sigma).pdf(q1)), ha='center', color='white')
                    ax.annotate('q3', xy=(q3, norm(mu, sigma).pdf(q3)), ha='center', color='white')
                    
                    ax.legend(loc="upper right", facecolor='white', fontsize=8)            
        
                    ax.set_facecolor('black')
                    ax.set_xlabel('Returns')
                    ax.set_ylabel('Probability Density')
                    ax.set_title('Distribution of Returns')
                    ax.set_xlim(-5, 5)
                    #ax.set_ylim(0, 1.2)
                    plt.show()
                    st.pyplot(fig)

            rolling_timeseries = st.checkbox('Plot rolling timeseries')

            if rolling_timeseries:
                
                fig, ax = plt.subplots(1,1)
                ax.plot(rolling_df['price'])
                ax.set_title('Price')
                plt.show()
                st.pyplot(fig)
                
                fig, ax = plt.subplots(1,1)
                ax.plot(rolling_df['mean'])
                ax.set_title('Rolling Mean')
                plt.show()
                st.pyplot(fig)
                
                fig, ax = plt.subplots(1,1)
                ax.plot(rolling_df['variance'])
                ax.set_title('Rolling Variance')
                plt.show()
                st.pyplot(fig)

                fig, ax = plt.subplots(1,1)
                ax.plot(rolling_df['skew'])
                ax.set_title('Rolling Skew')
                plt.show()
                st.pyplot(fig)
                
                fig, ax = plt.subplots(1,1)
                ax.plot(rolling_df['kurtosis'])
                ax.set_title('Rolling Kurtosis')
                plt.show()
                st.pyplot(fig)
                
                fig, ax = plt.subplots(1,1)
                ax.plot(rolling_df['hyperskew'])
                ax.set_title('Rolling Hyper Skew')
                plt.show()
                st.pyplot(fig)
                
                fig, ax = plt.subplots(1,1)
                ax.plot(rolling_df['hyperkurt'])
                ax.set_title('Rolling Hyper Kurtosis')
                plt.show()
                st.pyplot(fig)
                
                fig, ax = plt.subplots(1,1)
                ax.scatter(rolling_df['skew'], rolling_df['variance'], alpha=0.2)
                ax.set_title('Rolling Skew/Variance')
                plt.show()
                st.pyplot(fig)
                
                fig, ax = plt.subplots(1,1)
                ax.scatter(rolling_df['skew'], rolling_df['kurtosis'], alpha=0.2)
                ax.set_title('Rolling Skew/Kurtosis')
                plt.show()
                st.pyplot(fig)
                
                
                
        if group_analysis:
            
            dist_period = st.text_input('State distribution grouping period', '1D')
            index_groups = df.resample(dist_period)
            index_groups = list(index_groups.groups)
            group = len(df.loc[index_groups[1]:index_groups[2]])
            st.write('Every distribution of: ' + dist_period + ', will have: ' + str(group) + ' data points')
            
            df['groups'] = 'na'
            df['means'] = float(0)
            df['variances'] = float(0)
            df['skews'] = float(0)
            df['kurtosis'] = float(0)
            
            for s,e in zip(index_groups[:-1],index_groups[1:]):            
                df.loc[s:e]['groups'] = str(s) 
                df.loc[s:e]['means'] = df.returns[s:e].mean()
                df.loc[s:e]['variances'] = df.returns[s:e].var()
                df.loc[s:e]['skews'] = df.returns[s:e].skew()
                df.loc[s:e]['kurtosis'] = df.returns[s:e].kurt()
                
            df_groups= pd.DataFrame(list(zip(
                list(dict.fromkeys(df.groups)), 
                list(dict.fromkeys(df.means)), 
                list(dict.fromkeys(df.variances)), 
                list(dict.fromkeys(df.skews)),
                list(dict.fromkeys(df.iloc[:, 6])))),
                columns=['groups','means','variances', 'skews', 'kurtosis'])
                        
            group_distributions = st.checkbox('Plot grouped distributions')

            if group_distributions:
            
                for s,e in zip(index_groups[:-1], index_groups[1:]): 
                    end = str(df.loc[s:e]['returns'].dropna().index[-1])
                    start = str(df.loc[s:e]['returns'].dropna().index[0])
                    data = zscore(df.loc[s:e]['returns'].dropna())
                    
                    fig, ax = plt.subplots(1,1)
                    
                    mu = 0
                    sigma = 1
                    ax.hist(data, bins='auto', density = True, color = 'grey')
                    
                    pdf_plot = np.linspace(min(data), 
                                         max(data), 
                                         len(data))
                    
                    ax.plot(pdf_plot, norm.pdf(pdf_plot), linewidth=1, color='yellow', 
                            label=f'Start: {start}\nEnd: {end}\nMean: {round(data.mean(),5)}\nVariance: {round(data.var(),5)}\nSkew: {round(skew(data),3)}\nKurtosis: {round(kurtosis(data),3)}')
                    
                    median, q1, q3 = np.percentile(data, 50), np.percentile(data, 25), np.percentile(data, 75)
                                    
                    ax.vlines(median, ymin= 0, ymax= 0.8, color='r', linestyle='--', linewidth=0.5)                
                    ax.vlines(q1, ymin= 0, ymax= 0.8, color='b', linestyle='--', linewidth=0.5)                
                    ax.vlines(q3, ymin= 0, ymax= 0.8, color='b', linestyle='--', linewidth=0.5)            
                    
                    ax.annotate("{:.1f}%".format(100*norm(mu, sigma).cdf(q1)), xy=((q1-1.5*(q3-q1)+q1)/2, 0), ha='right', color='white')
                    ax.annotate("{:.1f}%".format(100*(norm(mu, sigma).cdf(q3)-norm(mu, sigma).cdf(q1))), xy=(median, 0), ha='center', color='white')
                    ax.annotate("{:.1f}%".format(100*(norm(mu, sigma).cdf(q3+1.5*(q3-q1)-q3)-norm(mu, sigma).cdf(q3))), xy=((q3+1.5*(q3-q1)+q3)/2, 0), ha='left', color='white')
                    ax.annotate('q1', xy=(q1, norm(mu, sigma).pdf(q1)), ha='center', color='white')
                    ax.annotate('q3', xy=(q3, norm(mu, sigma).pdf(q3)), ha='center', color='white')
                    
                    ax.legend(loc="upper right", facecolor='white', fontsize=8)            
        
                    ax.set_facecolor('black')
                    ax.set_xlabel('Returns')
                    ax.set_ylabel('Probability Density')
                    ax.set_title('Distribution of Returns')
                    ax.set_xlim(-5, 5)
                    #ax.set_ylim(0, 1.2)
                    plt.show()
                    st.pyplot(fig)
            
            group_timeseries = st.checkbox('Plot grouped timseries')

            if group_timeseries:
        
                fig = plt.figure(figsize=(10, 4))
                sns.lineplot(data=df_groups, x='groups', y="means")
                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
                st.pyplot(fig)
                
                fig = plt.figure(figsize=(10, 4))
                sns.lineplot(data=df_groups, x='groups', y="variances")
                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
                st.pyplot(fig)
                
                fig = plt.figure(figsize=(10, 4))
                sns.lineplot(data=df_groups, x='groups', y="skews")
                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
                st.pyplot(fig)
                
                fig = plt.figure(figsize=(10, 4))
                sns.lineplot(data=df_groups, x='groups', y="kurtosis")
                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
                st.pyplot(fig)
               
                fig = sns.displot(data=df, x="returns", hue="groups", kind="kde")
                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
                st.pyplot(fig)
            
        if reconstruction_analysis:  
            
            changing_period = st.text_input('A new distribution will be plotted every', '1D')
            small_index_groups = df.resample(changing_period)
            small_index_groups = list(small_index_groups.groups)
            small_rolling = len(df.loc[small_index_groups[0]:small_index_groups[1]]) - 1
            
            reconstruction = st.number_input('Reconstruct distribution after every x number of values', 10)
            
            reconstruction_plots = st.checkbox('Plot reconstruction plots')

            if reconstruction_plots:
            
                skews = []
                kurtoses = []
                dates = []
                for i in range(0, len(df), small_rolling): 
                    if i != 0:
                        plt.plot(df.price.iloc[i:i+small_rolling])
                        plt.show()
                        
                        df_plot = pd.DataFrame(list(zip(skews, kurtoses)), 
                                          index= pd.to_datetime(dates), 
                                          columns = ['skews','kurtoses'])
                        
                        fig, ax = plt.subplots(1,1)
                        ax.plot(df_plot['skews'])
                        ax.set_title('Reconstructed Skew')
                        plt.show()
                        st.pyplot(fig)
                        
                        fig, ax = plt.subplots(1,1)
                        ax.plot(df_plot['kurtoses'])
                        ax.set_title('Reconstructed Kurtosis')
                        plt.show()
                        st.pyplot(fig)
                        
                        skews = []
                        kurtoses = []
                        dates = []
    
                    for r in range(reconstruction, small_rolling, reconstruction):
                        #print(r)
                        end = str(df.iloc[i:r+i, 1].dropna().index[-1])
                        start = str(df.iloc[i:r+i, 1].dropna().index[0])
                        data = zscore(df.iloc[i:r+i, 1].dropna())
                        
                        skews.append(round(skew(data),3))
                        kurtoses.append(round(kurtosis(data),3))
                        dates.append(end)
    
                        fig, ax = plt.subplots(1,1)
                        
                        mu = 0
                        sigma = 1
                        ax.hist(data, bins='auto', density = True, color = 'grey')
                        
                        pdf_plot = np.linspace(min(data), 
                                             max(data), 
                                             len(data))
                        
                        ax.plot(pdf_plot, norm.pdf(pdf_plot), linewidth=1, color='yellow', 
                                label=f'Start: {start}\nEnd: {end}\nMean: {round(data.mean(),5)}\nVariance: {round(data.var(),5)}\nSkew: {round(skew(data),3)}\nKurtosis: {round(kurtosis(data),3)}')
                        
                        median, q1, q3 = np.percentile(data, 50), np.percentile(data, 25), np.percentile(data, 75)
                                        
                        ax.vlines(median, ymin= 0, ymax= 0.8, color='r', linestyle='--', linewidth=0.5)                
                        ax.vlines(q1, ymin= 0, ymax= 0.8, color='b', linestyle='--', linewidth=0.5)                
                        ax.vlines(q3, ymin= 0, ymax= 0.8, color='b', linestyle='--', linewidth=0.5)            
                        
                        ax.annotate("{:.1f}%".format(100*norm(mu, sigma).cdf(q1)), xy=((q1-1.5*(q3-q1)+q1)/2, 0), ha='right', color='white')
                        ax.annotate("{:.1f}%".format(100*(norm(mu, sigma).cdf(q3)-norm(mu, sigma).cdf(q1))), xy=(median, 0), ha='center', color='white')
                        ax.annotate("{:.1f}%".format(100*(norm(mu, sigma).cdf(q3+1.5*(q3-q1)-q3)-norm(mu, sigma).cdf(q3))), xy=((q3+1.5*(q3-q1)+q3)/2, 0), ha='left', color='white')
                        ax.annotate('q1', xy=(q1, norm(mu, sigma).pdf(q1)), ha='center', color='white')
                        ax.annotate('q3', xy=(q3, norm(mu, sigma).pdf(q3)), ha='center', color='white')
                        
                        ax.legend(loc="upper right", facecolor='white', fontsize=8)            
            
                        ax.set_facecolor('black')
                        ax.set_xlabel('Returns')
                        ax.set_ylabel('Probability Density')
                        ax.set_title('Distribution of Returns')
                        ax.set_xlim(-5, 5)
                        #ax.set_ylim(0, 1.2)
                        plt.show()
                        st.pyplot(fig)
          
        if whole_analysis:
            
            df = df['returns'].replace([np.inf, -np.inf], np.nan).dropna()
            
            def plot(df, data_stat):
                """ 
                Histogram of the input data. Contains also information about the 
                Figure style, depending on the active Mode.
                """
                
                if plot_mode == 'Light Mode':
                    hist_edge_color = 'black'
                    hist_color= 'white'
                    var_color = 'blue'
                    skew_color = 'green'
                    kurt_color = 'purple'
                    quant_color = 'black'
                    median_color = 'black'
                    pdf_color = '#08519c'
                    cdf_color = 'black'
                    plt.style.use('classic')
                    plt.rcParams['figure.facecolor'] = 'white'
                    
                if plot_mode == 'Dark Mode':
                    hist_edge_color = 'black'
                    hist_color= 'white'
                    var_color = 'blue'
                    skew_color = 'green'
                    kurt_color = 'purple'
                    quant_color = 'white'
                    pdf_color = '#fec44f'
                    cdf_color = 'white'
                    plt.style.use('dark_background')
                    plt.rcParams['figure.facecolor'] = 'black'
                    
                fig, ax = plt.subplots(1,1)
                
                # Plot hist
                ax.hist(df, bins=round(math.sqrt(len(df))), 
                        density=True, color=hist_color, 
                        ec=hist_edge_color, alpha=0.3)
            
                ax.spines['right'].set_visible(False)
                ax.spines['top'].set_visible(False)
            
                plt.tick_params(top=False, bottom=True, left=True, right=False,
                        labelleft=True, labelbottom=True)
            
                ax.set_xlabel('Returns')
                ax.set_ylabel('Probability')
                
                # If user selects data_stat
                if data_stat:
                    # Hist contains tuple: n bins, (n+1) bin boundaries
                    hist = np.histogram(df, bins=round(math.sqrt(len(df))))
                    #Generates a distribution given by a histogram.
                    hist_dist = scipy.stats.rv_histogram(hist)
                    x_plot = np.linspace(min(df), max(df), len(df))
            
            
                    q = [0.05, 0.25, 0.50, 0.75, 0.95]
                    n = ['5th','25th','50th','75th','95th']
                    quantiles = df.quantile(q)
                    q_max = hist_dist.cdf(quantiles)
            
                    
                    for i, qu in enumerate(quantiles):
                        ax.plot(qu, q_max[i], alpha=0.5, color=quant_color,
                                markersize=10, marker='D')
                        ax.text(qu, q_max[i]+(q_max[i]/10), f'{n[i]}', ha='center',
                                alpha=0.5)
                    ax.scatter([], [], alpha=0.5, color=quant_color, marker='D', 
                               label='Percentiles')
                    # The pdf is defined as a stepwise function from the provided histogram.
                    # The cdf is a linear interpolation of the pdf.
                    ax.plot(x_plot, hist_dist.pdf(x_plot), linewidth=2,
                            color=pdf_color, label='PDF')
                    ax.plot(x_plot, hist_dist.cdf(x_plot), linewidth=2,
                            color=cdf_color, label='CDF')
                    
                    
                    ax.vlines(np.mean(df), ymin=0, ymax=hist_dist.cdf(np.mean(df)),
                              color='red', linestyle='--', linewidth=2,
                              label=f'Mean {round(np.mean(df),3)}\nSkew {round(skew(df),3)}\nVariance {round(np.var(df),3)}\nKurtosis {round(kurtosis(df),3)}')
                    
                    leg = plt.legend(loc=0)
                    leg.get_frame().set_edgecolor("#525252")
            
                return fig  
            
            
            def bokeh_set_plot_properties(plot_mode, n):
                """
                Constructs a list of properties that will be assigned to a Bokeh
                figure depending whether it is in the Light or Dark Mode.
                Parameters
                ----------
                plot_mode : string; plot 'Dark Mode' or 'Light Mode'
                Returns
                -------
                p : Bokeh Figure
                colors_cmr : Colors from the colormap to be assigned to lines
                """
                        
                p = figure(height=450, width=700)
                
                p.add_layout(Legend(), 'right')
                p.legend.title = '15 Best Fits and their SSE'
                p.legend.background_fill_alpha = 1
                p.legend.label_text_font_size = '11pt'
                p.xgrid.grid_line_color = None
                p.ygrid.grid_line_color = None
                p.xaxis.axis_label = 'Returns'
                p.yaxis.axis_label = 'Probability' 
            
            
                if plot_mode == 'Dark Mode':
                    text_color = 'white'
                    back_color = 'black'
                    legend_color = 'yellow'
                    
                    # It will get n colors from cmasher rainforest map
                    # if n>15, it will take 15; otherwise n will be the 
                    # lengthe of the chosed distributions (n defined line 685)
                    colors_cmr = cmr.take_cmap_colors('cmr.rainforest_r', 
                                              n, cmap_range=(0.1, 0.7), 
                                             return_fmt='hex')     
                
                if plot_mode == 'Light Mode':
                    text_color = 'black'
                    back_color = 'white'
                    legend_color = 'blue'   
                
                    colors_cmr = cmr.take_cmap_colors('cmr.rainforest', 
                                              n, cmap_range=(0.2, 0.9), 
                                             return_fmt='hex')
                
                p.legend.title_text_color = text_color
                p.yaxis.major_label_text_color = text_color
                p.xaxis.axis_label_text_color = text_color
                p.xaxis.major_label_text_color = text_color
                p.yaxis.axis_label_text_color = text_color
                p.xaxis.major_tick_line_color = text_color
                p.yaxis.major_tick_line_color = text_color
                p.xaxis.minor_tick_line_color = text_color
                p.yaxis.minor_tick_line_color = text_color
                p.xaxis.axis_line_color = text_color
                p.yaxis.axis_line_color = text_color
                
                p.border_fill_color = back_color
                p.background_fill_color = back_color
                p.legend.background_fill_color = back_color
                p.legend.label_text_color = legend_color
                p.title.text_color = legend_color
                p.outline_line_color = back_color
                
                return p, colors_cmr
               
              
            def bokeh_pdf_plot_results(df, results, n):
                """
                Process results and plot them on the Bokeh Figure. User can interact
                with the legend (clicking on the items will enhance lines on Figure)
                Parameters
                ----------
                df : input data
                results : nested list (contains tuples) with the data from the 
                        fitting (contains [sse, arg, loc, scale])
                n : integer; First n best fit PDFs to show on the Figure.
                plot_mode : string; 'Dark Mode' or 'Light Mode' (connected with radio
                                                                 button)
                Returns
                -------
                p : Returns Bokeh interactive figure (data histogram+best fit PDFs)
                """
                         
                # Pasing dictionary with best fit results
                fit_dict_res = fit_data(df)
                hist, edges = np.histogram(df, density=True, 
                                           bins=round(math.sqrt(len(df))))
                
            
                # Obtain Figure mode from the function:  bokeh_set_plot_properties
                p, colors_cmr = bokeh_set_plot_properties(plot_mode, n)
                
                # Bokeh histogram
                p.quad(top=hist, bottom=0, left=edges[:-1], 
                       right=edges[1:], line_color="black",
                       line_width = 0.3,
                       fill_color='white', fill_alpha = 0.3)
                
                # Plot each fitted distribution
                i = -1
                for distribution, result in fit_dict_res.items():
                    i += 1
                    
                    sse = round(result[0],2)
                    arg = result[1]
                    loc = result[2]
                    scale = result[3]
            
                    best_params = result[1:4] 
                    flat_list = list(flatten(best_params)) 
                    param_names = (distribution.shapes + ', loc, scale').split(', ') if distribution.shapes else ['loc', 'scale']
                    param_str = ', '.join([f'{k} = {round(v,2)}' for k,v 
                                           in zip(param_names, flat_list)])
            
                    # Generate evenly spaced numbers over a specified interval
                    # Make pdf/cdf with the parameters of fitted functions
                    x_plot = np.linspace(min(df), max(df), 400)
                    y_plot = distribution.pdf(x_plot, loc=loc, scale=scale, *arg)
            
                    # The best fit distribution will be with i=0
                    if i == 0:
                        # Bokeh line plot with interactive legend
                        line = p.line(x_plot, y_plot, line_width=5,
                               line_color = colors_cmr[0],
                               legend_label=str(distribution.name) + ": " + str(sse)
                               )
                        line.visible = True
                        p.legend.click_policy = "hide"
                        p.title.text = f'Best fit {distribution.name}: {param_str}'
            
                    # Next 15 best fits; 15 is arbitrary taken.
                    elif (i>0) and (i < 15):
                        lines = p.line(x_plot, y_plot, line_width=2.5,
                                        line_dash="10 2",
                               line_color = colors_cmr[i],
                               legend_label =str(distribution.name) + ": " + str(sse)
                                )
                        lines.visible = False
                        p.legend.click_policy = "hide"
            
                    else:
                        pass
                           
                return p  
            
            
            def bokeh_cdf_plot_results(df, results, n):
                """
                Process results and plot them on the Bokeh Figure. User can interact
                with the legend (clicking on the items will enhance lines on Figure)
                Parameters
                ----------
                df : input data
                results : nested list (contains tuples) with the data from the 
                        fitting (contains [sse, arg, loc, scale])
                n : integer; First n best fit CDFs to show on the Figure.
                plot_mode : string; 'Dark Mode' or 'Light Mode' (connected with radio
                                                                 button)
                Returns
                -------
                p : Returns Bokeh interactive figure (data hostogram+best fit CDFs)
                """
                
                # Hist contains tuple: n bins, (n+1) bin boundaries
                hist_data = np.histogram(df, bins=round(math.sqrt(len(df))))
                #Generates a distribution given by a histogram.
                hist_dist_data = scipy.stats.rv_histogram(hist_data)
                x_plot_data = np.linspace(min(df), max(df), 400)
                  
            
                # Pasing dictionary with best fit results
                fit_dict_res = fit_data(df)
                
                hist, edges = np.histogram(df, density=True, 
                                           bins=round(math.sqrt(len(df))))
                
            
                # Obtain Figure mode from the function:  bokeh_set_plot_properties
                p, colors_cmr = bokeh_set_plot_properties(plot_mode, n)
                
                # Bokeh histogram
                p.quad(top=hist, bottom=0, left=edges[:-1], 
                       right=edges[1:], line_color="black",
                       line_width = 0.3,
                       fill_color='white', fill_alpha = 0.3)
                
                p.line(x_plot_data, hist_dist_data.cdf(x_plot_data), 
                                  line_color='red', legend_label='CDF sample data',
                                  line_width=3)  
                p.legend.click_policy = "hide"
            
                # Plot each fitted distribution
                i = -1
                for distribution, result in fit_dict_res.items():
                    i += 1
                    
                    sse = round(result[0],2)
                    arg = result[1]
                    loc = result[2]
                    scale = result[3]
            
                    best_params = result[1:4] 
                    flat_list = list(flatten(best_params)) 
                    param_names = (distribution.shapes + ', loc, scale').split(', ') if distribution.shapes else ['loc', 'scale']
                    param_str = ', '.join([f'{k} = {round(v,2)}' for k,v 
                                           in zip(param_names, flat_list)])
            
                    # Generate evenly spaced numbers over a specified interval
                    # Make pdf/cdf with the parameters of fitted functions
                    x_plot = np.linspace(min(df), max(df), len(df))
            
                    y_plot = distribution.cdf(x_plot, loc=loc, scale=scale, *arg)
                        
                        
                    # The best fit distribution will be with i=0
                    if i == 0:
                        # Bokeh line plot with interactive legend
                        line = p.line(x_plot, y_plot, line_width=5,
                               line_color = colors_cmr[0],
                               legend_label=str(distribution.name) + ": " + str(sse)
                               )
                        line.visible = True
                        p.legend.click_policy = "hide"
                        p.title.text = f'Best fit {distribution.name}: {param_str}'
            
                    # Next 15 best fits; 15 is arbitrary taken.
                    elif (i>0) and (i < 15):
                        lines = p.line(x_plot, y_plot, line_width=2.5,
                                        line_dash="10 2",
                               line_color = colors_cmr[i],
                               legend_label =str(distribution.name) + ": " + str(sse)
                                )
                        lines.visible = False
                        p.legend.click_policy = "hide"
            
                    else:
                        pass
                           
                return p
            
            
            @st.cache(allow_output_mutation=True)
            def fit_data(df):
                """ 
                Modified from: https://stackoverflow.com/questions/6620471/fitting\
                    -empirical-distribution-to-theoretical-ones-with-scipy-python 
                
                This function is performed with @cache - storing results in the local
                cache; read more: https://docs.streamlit.io/en/stable/caching.html
                """
                
                # If the distribution(s) are selected in the selectbox
                if chosen_distr:
                    
                    # Check for nan/inf and remove them
                    ## Get histogram of the data and histogram parameters
                    num_bins = round(math.sqrt(len(df)))
                    hist, bin_edges = np.histogram(df, num_bins, density=True)
                    central_values = np.diff(bin_edges)*0.5 + bin_edges[:-1]
            
                    results = {}
                    for distribution in chosen_distr:
                        
                        # Go through distributions
                        dist = getattr(scipy.stats, distribution)
                        # Get distribution fitted parameters
                        params = dist.fit(df)
                        
                        ## Separate parameters
                        arg = params[:-2]
                        loc = params[-2]
                        scale = params[-1]
                    
                        ## Obtain PDFs
                        pdf_values = [dist.pdf(c, loc=loc, scale=scale, *arg) for c in
                                      central_values]
                
                        # Calculate the RSS: residual sum of squares 
                        # Also known as SSE: sum of squared estimate of errors
                        # The sum of the squared differences between each observation\
                        # and its group's mean; here: diff between distr. & data hist
                        sse = np.sum(np.power(hist - pdf_values, 2.0))
                        
                        
                        # Parse fit results 
                        results[dist] = [sse, arg, loc, scale]
            
                    # containes e.g. [0.5, (13,), 8, 1]
                    # equivalent to  [sse, (arg,), loc, scale] as argument number varies
                    results = {k: results[k] for k in sorted(results, key=results.get)}
            
                    return results
            
            def flatten(nested_l):
                
                """
                Flatten the list and take care if there are tuples in the list.
                Arguments can be multiple (a, b, c...). This function is from:
                https://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists
                """
                if isinstance(nested_l, collections.Iterable):
                    return [a for i in nested_l for a in flatten(i)]
                else:
                    return [nested_l]
                
            
            def results_to_dataframe(df, results):
                """ 
                This function takes the results from the fitting and parses it to 
                produce variables that will be storred into PandasDataframe for
                the easy overview.
                """
                
                # Pasing dictionary with best fit results
                fit_dict_res = fit_data(df)
                
                df_dist = []
                df_params = []
                df_sse = []
                for distribution, result in fit_dict_res.items():
                    sse = result[0]
                    best_params = result[1:4] 
            
                    flat_list = list(flatten(best_params))
                    
                    param_names = (distribution.shapes + ',loc,scale').split(',') if distribution.shapes else ['loc', 'scale']
                    param_str = ', '.join([f'{k} = {round(v,2)}' for k,v 
                                           in zip(param_names, flat_list)])
                    
                    #title = f'{distribution.name}: {param_str}'
                    
                    df_dist.append(f'{distribution.name}')
                    df_params.append(f'{param_str}')
                    df_sse.append(round(sse, 4))
            
                fit_results = pd.DataFrame(
                        {'Distribution': df_dist,
                         'Fit Parameters': df_params,
                         'SSE': df_sse}
                        )
            
                return fit_results 
            
            def produce_output_for_code_download_parameters(df, results):
                """
                Parse the best fit function and parameters to generate python
                code for User. Works fine for all current forms of the 
                continuous functions (with various numbers of shape parameters).
                """
                
                # Need to start loop as I want to separate first item
                i = -1
                # Pasing dictionary with best fit results
                fit_dict_res = fit_data(df)
                for distribution, result in fit_dict_res.items():
                    i += 1
                    if i == 0:
                        # Need to add to to check if there are shapes or not
                        if distribution.shapes is not None:
                            shapes = distribution.shapes+str(',')
                        else:
                            shapes = ""
                        #print(shapes)
                    else:
                        pass
                df_results = results_to_dataframe(df, results)
                
                best_dist = df_results['Distribution'][0]
                
                fit_params_all = df_results['Fit Parameters']
                  
                # Get scale
                best_scale = fit_params_all[0].split(", ")[-1]
                # Get loc 
                best_loc = fit_params_all[0].split(", ")[-2]
                # Get all arguments
                args = fit_params_all[0].split(",")[0:-2]
                # String manipulation to matches desired form for the code generation
                args = str([i for i in args]).strip(" [] ").strip("'").replace("'", '').replace(" ", '').replace(",",'\n')
            
                return shapes, best_dist, best_scale, best_loc, args, fit_params_all[0]
            
            def py_file_downloader(py_file_text):
                """
                Strings <-> bytes conversions and creating a link which will
                download generated python script.
                """
            
                # Add a timestamp to the name of the saved file
                time_stamp = time.strftime("%Y%m%d_%H%M%S")
            
                # Base64 takes care of porting info to the data
                b64 = base64.b64encode(py_file_text.encode()).decode()
                
                # Saved file will have distribution name and the timestamp
                code_file = f"{best_dist}_{time_stamp}.py"
                st.markdown(f'** Download Python File **: \
                            <a href="data:file/txt;base64,{b64}" \
                                download="{code_file}">Click Here</a>', 
                                unsafe_allow_html=True)
            
            def csv_downloader(data):
                """
                Strings <-> bytes conversions and creating a link which will
                download generated csv file with the DataFrame that contains fitting
                results.
                """
                time_stamp = time.strftime("%Y%m%d_%H%M%S")
                
                csvfile = data.to_csv()
                
                b64 = base64.b64encode(csvfile.encode()).decode()
                
                new_filename = f"fit_results{time_stamp}.csv"
                href = f'** Download DataFrame as .csv: ** \
                    <a href="data:file/csv;base64,{b64}" \
                    download="{new_filename}">Click Here</a>'
                st.markdown(href, unsafe_allow_html=True)
            
            # Distribution names
            dis = distr_selectbox_names()
            
            # Checks steps by steps to ensure the flow of the data input; examination,
            # fitting and the display of the results.
            if input:
            
                st.write("Examine your data:")
                
                data_plot = st.checkbox('Plot my data')
                data_stat = st.checkbox("Sample statistics")
                if data_plot:
                    st.pyplot(plot(df, data_stat))
                
                # Add an option to have a 'Select All' distribution
                dis_with_all =[]
                dis_with_all = dis[:]
                dis_with_all.append('All_distributions')
            
                chosen_distr = st.multiselect('Choose distributions to fit', 
                                              dis_with_all)
                # Remove problematic distributions
                if 'All_distributions' in chosen_distr:
                    dis.remove('levy_stable')
                    dis.remove('kstwo')
                    chosen_distr = dis
                  
                # Give warnings if User selects problematic distributions
                if chosen_distr:
                    if 'kstwo' in chosen_distr:
                        st.warning("User, be aware that **kstwo**\
                                   distribution has some issues and will not compute.")
                    
                    if 'levy_stable' in chosen_distr:
                        st.warning("User, be aware that **levy_stable**\
                                   distribution will take several minutes to compute.")
                    
                    if chosen_distr == dis:
                        st.warning(" You have selected **All distributions**, due to \
                                   the slow computation of the *levy_stable* \
                                    (and errors with *kstwo*), \
                                    these distributions are removed\
                                    from the list of 'All_distributions' ")
                    
                    st.write("Do you want to fit the selected distribution(s) \
                             to your data?")
                    
                    # Checking length of selected distributions, for a number colors
                    # that will be taken from colormap. As plot gets messy with more
                    # than 15, I limit to that; if smaller, use that number
                    if len(chosen_distr) > 15:
                        n = 15
                    else:
                        n = len(chosen_distr)
                        
                    # Fit
                    fit_confirmation =  st.checkbox("Yes, please.", value=False)
                    
                    if fit_confirmation:
                        with st.spinner("Fitting... Please wait a moment."):
                            results = fit_data(df)
                                        
                    # After fitting, checkbox apears and when clicked: user get 
                    # options which results they want to see, as they are several
                    if fit_confirmation:
                        st.write('Results are ready, select what you wish to see:')   
            
                        if st.checkbox('Interactive Figures'):
                            st.info('Interactive Figure: click on the legend to \
                                enhance selected fit.')
                            p1 =  bokeh_pdf_plot_results(df, results, n) #p2
                            st.bokeh_chart(p1)
                            st.info('Interactive Figure: Comparing CDFs')
                            p2 =  bokeh_cdf_plot_results(df, results, n)
                            st.bokeh_chart(p2)
                        
                        if st.checkbox('Table'):
                            st.info('DataFrame: all fitted distributions\
                                and their SSE (sum of squared estimate of errors).')
                            st.dataframe(results_to_dataframe(df, results))
                            csv_downloader(results_to_dataframe(df, results))
                            
            
                        shapes, best_dist, best_scale, best_loc, args, fit_params_all \
                            = produce_output_for_code_download_parameters(df, results)
                        
            
