# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 23:05:14 2016

@author: raghuios7
"""
from pandas import DataFrame
from datetime import datetime, timedelta
import json
import numpy as np
import sys
import getopt
import warnings

def calculateVenmoMedian(inputfile, outputfile):

    maxDT = ""
    minDT = ""

    #Read all the payments from the input file
    payments = [json.loads(line) for line in open(inputfile,'rb')]
    outputfileobj = open(outputfile,'w')
    
    #Get all the payments to a DataFrame object
    df = DataFrame(payments)

    #Exclude rows which have empty actor, target and created_time fields    
    #First replace the empty cells to NaN and then drop the rows with NaN cells
    df = df.apply(lambda y: y.str.strip() if isinstance(y,str) else y).replace('',np.nan)
    df = df.dropna()
    
    #Convert the created_time column to datetime object
    df['created_time'] = df['created_time'].apply(lambda x:datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))

    #Process every payment one by one
    for index, row in df.iterrows():

        #Get the max date/time
        if maxDT:
            if row['created_time'] > maxDT:
                maxDT = row['created_time']
                minDT = maxDT - timedelta(seconds=60)
        else:
            maxDT = row['created_time']
            minDT = maxDT - timedelta(seconds=60)
            
        #Process the next payment
        #Add it to a running list of payments    
        df_0 = df[0:index+1]
    
        #Get a set of payments which are between minimum and maximum date/time
        df_1 = df_0[(df_0['created_time'] > minDT) & (df_0['created_time'] <= maxDT)]

        #Remove duplicate edges
        #This logic will keep only one edge between two nodes
        #For example: for edges X-Y, X-Y and Y-X only one edge will be preserved X-Y in this case
        mask = df_1['actor'] < df_1['target']
        df_1['first'] = df_1['actor'].where(mask, df_1['target'])
        df_1['second'] = df_1['target'].where(mask, df_1['actor'])
        df_1 = df_1.drop_duplicates(['first','second'])
        #recreate the dataframe with only two columns (actor and target)
        df_1 = df_1[['actor','target']]
       
        #Also drop if actor is equal target - self edges (self payments?)
        df_1 = df_1.drop(df_1[df_1['actor']==df_1['target']].index)

        #Group the actors/targets and get the counts
        series_actor = df_1['actor'].value_counts()
        series_target = df_1['target'].value_counts()
        
        #Merge both the actor/target counts
        series_edges = series_actor.add(series_target,fill_value=0)
        #print series_edges        
        
        #Sort the edges
        series_edges_sorted = series_edges.sort_values()
    
        #Find the median
        venmo_median = np.nanmedian(series_edges_sorted, 0)

        #Write to the output file        
        outputfileobj.write("%.2f\n" %venmo_median)
    
    outputfileobj.close()

def main(argv):

    inputfile = ""
    outputfile = ""

    try:
            opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
            print 'python rolling_median.py -i <inputfile> -o <outputfile>'
            sys.exit(2)

    for opt, arg in opts:
            if opt == '-h':
                    print 'python rolling_median.py -i <inputfile> -o <outputfile>'
                    sys.exit()
            elif opt in ("-i","--ifile"):
                    inputfile = arg
            elif opt in ("-o","--ofile"):
                    outputfile = arg
                    
    calculateVenmoMedian(inputfile, outputfile)                    


if __name__=="__main__":
        warnings.simplefilter("ignore")
        main(sys.argv[1:])
