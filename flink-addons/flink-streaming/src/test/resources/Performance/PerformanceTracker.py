# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 15:40:17 2014

@author: gyfora
"""

import matplotlib.pyplot as plt
import pandas as pd
import os

def plotPerformance(csv_dir):
    csv_dir=csv_dir
    dataframes={}
    
    for fname in os.listdir(csv_dir):
        if '.csv' in fname:
            df=pd.read_csv(os.path.join(csv_dir,fname),index_col='Time')
            speed=[0]
            values=list(df.ix[:,0])
            for i in range(1,len(values)):
                speed.append(float(values[i]-values[i-1])/float(df.index[i]-df.index[i-1]))
            df['speed']=speed    
            dataframes[fname.rstrip('.csv')]=df
            
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('Values')
    
    for name in dataframes.keys():
        dataframes[name].ix[:,0].plot()
    plt.legend(dataframes.keys())
    
    plt.figure(figsize=(12, 8), dpi=80)
    plt.title('dV/dT')

    for name in dataframes.keys():
        dataframes[name].speed.plot()
    plt.legend(dataframes.keys())
