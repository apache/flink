# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 15:40:17 2014

@author: gyfora
"""

import matplotlib.pyplot as plt
import pandas as pd
import os

linestyles = ['_', '-', '--', ':']
markers=['x','o','^','+']
def readFiles(csv_dir):
    dataframes={}
    machine=[]
    
    for fname in os.listdir(csv_dir):
        if '.csv' in fname:
            dataframes[fname.rstrip('.csv')]=pd.read_csv(os.path.join(csv_dir,fname),index_col='Time')
            machine.append(int(fname.rstrip('.csv')[-1]))
    return dataframes,machine
    
def plotCounter(csv_dir, smooth=5):
    dataframes,machine= readFiles(csv_dir)
        
    
    for name in dataframes:
        df=dataframes[name]
        speed=[0]
        values=list(df.ix[:,0])
        for i in range(1,len(values)):
            speed.append(float(values[i]-values[i-1])/float(df.index[i]-df.index[i-1]))
        df['speed']=speed 
        
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('Counter')
    
    for name in enumerate(dataframes):
        if len(markers)>machine[name[0]]:
            m=markers[machine[name[0]]]            
        else: m='*'  

        dataframes[name[1]].ix[:,0].plot(marker=m,markevery=10,markersize=10)
    plt.legend(dataframes.keys())
    
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('dC/dT')

    for name in enumerate(dataframes):
        if len(markers)>machine[name[0]]:
            m=markers[machine[name[0]]]            
        else: m='*'
        pd.rolling_mean(dataframes[name[1]].speed,smooth).plot(marker=m,markevery=10,markersize=10)
    plt.legend(dataframes.keys())
        
        

def plotTimer(csv_dir,smooth=5,std=50):
    dataframes,machine= readFiles(csv_dir)
    
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('Timer')
    
    for name in dataframes:
        pd.rolling_mean(dataframes[name].ix[:,0],smooth).plot()
    plt.legend(dataframes.keys())
    
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('Standard deviance')

    for name in dataframes:
        pd.rolling_std(dataframes[name].ix[:,0],std).plot()
    plt.legend(dataframes.keys())
