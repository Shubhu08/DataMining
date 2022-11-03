#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 02:15:35 2021

@author: shubhashreedash
"""

from copy import deepcopy
from itertools import product, combinations
from pyspark import SparkContext
from math import ceil
import sys
import time

if __name__ == "__main__":
    filter_threshold = int(sys.argv[1])
    support_threshold = int(sys.argv[2])
    input_file_path = sys.argv[3] # "/Users/shubhashreedash/Documents/USC/Summer Sem 2021/Data Mining/Assignment 2/data/small1.csv" #
    output_file_path = sys.argv[4]
    
    #set_try = set([('1','2'),('2','3'),('1','3'),('2','4'),('1','2','3'),('2','3','4'),('2','4'),('1','2','4'),('1','2','5','4'),('1','2','3'),('1','2','3','4','5'),('1','2','6','4','5')])
    #a_priori(set_try,2)
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')

    start_time = time.time()
    
    input_rdd = sc.textFile(input_file_path,10)
    header = input_rdd.first()
    #print("Header : ",header)
    input_rdd = input_rdd.filter(lambda line : line != header)\
        .map(lambda line: (line.split(',')[0], (line.split(',')[1],)))\
        .reduceByKey(lambda vals , val1 : val1 + vals)\
        .filter(lambda item : len(item[1]) > filter_threshold)\
        .map(lambda item : item[1])