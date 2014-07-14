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
    counters=[]
				
    for fname in os.listdir(csv_dir):
        if '.csv' in fname:
            counters.append((fname.rstrip('.csv'),int(fname.rstrip('.csv').split('-')[-1])-1,pd.read_csv(os.path.join(csv_dir,fname),index_col='Time')))
    return counters
    
def plotCounter(csv_dir, sname='', smooth=5,savePath=''):
	counters= readFiles(csv_dir)
	addSpeed(counters)	 
    
        
	
    
    
	selectedCounters=[]
	for (name, number, df) in counters:
		if sname in name:
			selectedCounters.append((name, number, df))
	if sname=='':
		sname='counters'
	save=savePath!=''	
	
	plotDfs(selectedCounters,smooth,save,savePath+'/'+sname)
								
def plotDfs(counters,smooth,save,saveFile):
        plt.figure(figsize=(12, 8), dpi=80)
        plt.title('Counter')
        for (name, number, df) in counters:			
            
            m=markers[number%len(markers)]    
              
            df.ix[:,0].plot(marker=m,markevery=10,markersize=10)
        plt.legend([x[0] for x in counters])
        if save:        
		plt.savefig(saveFile+'C.png')
									
									
        plt.figure(figsize=(12, 8), dpi=80)
        plt.title('dC/dT')
       
    
        for (name, number, df) in counters:
            
            m=markers[number%len(markers)]            
            
            pd.rolling_mean(df.speed,smooth).plot(marker=m,markevery=10,markersize=10)
        plt.legend([x[0] for x in counters])
        if save:
		plt.savefig(saveFile+'D.png')
								
def addSpeed(counters):
	for (tname, number, df) in counters:
		speed=[0]
		values=list(df.ix[:,0])
		for i in range(1,len(values)):
			speed.append(float(values[i]-values[i-1])/float(df.index[i]-df.index[i-1]+0.01))
		df['speed']=speed
	return counters
        
def plotThroughput(csv_dir,tasknames, smooth=5,savePath=''):
	if type(tasknames)!=list:
		tasknames=[tasknames]
	for taskname in tasknames:
		counters= readFiles(csv_dir)
		addSpeed(counters)
		selected={}
	    
		for (tname, number, df) in counters:
			if taskname in tname:
				if number in selected:
			                selected[number].append(df)
				else:
			                selected[number]=[df]
		plt.figure()    
		plt.title(taskname)
		for i in selected:
			if len(selected[i])>1:
				selected[i]=reduce(operator.add,selected[i])
			else:
				selected[i]=selected[i][0]
			m=markers[i%len(markers)]       
			selected[i].ix[:,0].plot(marker=m,markevery=10,markersize=10)
	
	        
		plt.legend(selected.keys())
		if savePath !='':
			plt.savefig(savePath+'/'+taskname+'C.png')    
		plt.figure()    
		plt.title(taskname+" - dC/dT")
		for i in selected:
			m=markers[i%len(markers)]       
			pd.rolling_mean(selected[i].speed,smooth).plot(marker=m,markevery=10,markersize=10)
	        
		plt.legend(selected.keys())
		if savePath !='':
			plt.savefig(savePath+'/'+taskname+'D.png') 
    
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
