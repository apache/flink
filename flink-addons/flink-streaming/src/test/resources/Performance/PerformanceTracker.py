# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 15:40:17 2014

@author: gyfora
"""

import matplotlib.pyplot as plt
import pandas as pd
import os
import operator


linestyles = ['_', '-', '--', ':']
markers=['D','s', '|', '', 'x', '_', '^', ' ', 'd', 'h', '+', '*', ',', 'o', '.', '1', 'p', 'H', 'v', '>'];
colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
def readFiles(csv_dir):
    dataframes=[]
    
    
    for fname in os.listdir(csv_dir):
        if '.csv' in fname:
            dataframes.append((fname.rstrip('.csv'),int(fname.rstrip('.csv').split('-')[-1])-1,pd.read_csv(os.path.join(csv_dir,fname),index_col='Time')))
    return dataframes
    
def plotCounter(csv_dir, smooth=5):
    dataframes= readFiles(csv_dir)
        
    
    for dataframe in dataframes:
        df=dataframe[2]
        speed=[0]
        values=list(df.ix[:,0])
        for i in range(1,len(values)):
            speed.append(float(values[i]-values[i-1])/float(df.index[i]-df.index[i-1]))
        df['speed']=speed 
        
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('Counter')
    
    for dataframe in dataframes:
        
        m=markers[dataframe[1]%len(markers)]    
          
        dataframe[2].ix[:,0].plot(marker=m,markevery=10,markersize=10)
    plt.legend([x[0] for x in dataframes])
    
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('dC/dT')

    for dataframe in dataframes:
        
        m=markers[dataframe[1]%len(markers)]            
        
        pd.rolling_mean(dataframe[2].speed,smooth).plot(marker=m,markevery=10,markersize=10)
    plt.legend([x[0] for x in dataframes])
        
def plotThroughput(csv_dir,taskname, smooth=5):
    dataframes= readFiles(csv_dir)

    for dataframe in dataframes:
        df=dataframe[2]
        speed=[0]
        values=list(df.ix[:,0])
        for i in range(1,len(values)):
            speed.append(float(values[i]-values[i-1])/float(df.index[i]-df.index[i-1]))
        df['speed']=speed     
    
    selected={}
    
    for df in dataframes:
        if taskname in df[0]:
            if df[1] in selected:
                selected[df[1]].append(df[2])
            else:
                selected[df[1]]=[df[2]]
    plt.figure()    
    plt.title(taskname)
    for i in selected:
        selected[i]=reduce(operator.add,selected[i])
        m=markers[i%len(markers)]       
        selected[i].ix[:,0].plot(marker=m,markevery=10,markersize=10)

        
    plt.legend(selected.keys())
    
    plt.figure()    
    plt.title(taskname+" - dC/dT")
    for i in selected:
        m=markers[i%len(markers)]       
        pd.rolling_mean(selected[i].speed,smooth).plot(marker=m,markevery=10,markersize=10)
        
    plt.legend(selected.keys())
    
def plotTimer(csv_dir,smooth=5,std=50):
    dataframes= readFiles(csv_dir)
    
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('Timer')
    
    for dataframe in dataframes:
        
        m=markers[dataframe[1]%len(markers)]  
          
        pd.rolling_mean(dataframe[2].ix[:,0],smooth).plot(marker=m,markevery=10,markersize=10)
    plt.legend([x[0] for x in dataframes])
    
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('Standard deviance')

    for dataframe in dataframes:
        
        m=markers[dataframe[1]%len(markers)]   
         
        pd.rolling_std(dataframe[2].ix[:,0],std).plot(marker=m,markevery=10,markersize=10)
    plt.legend([x[0] for x in dataframes])
