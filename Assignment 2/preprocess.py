#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 02:15:35 2021

@author: shubhashreedash
"""

from pyspark import SparkContext
import json
import sys
import csv

if __name__ == "__main__":
    review_input_json = sys.argv[1]
    business_input_json = sys.argv[2]
    output_file_path = sys.argv[3]
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')

       
    business_rdd = sc.textFile(business_input_json)\
        .map(lambda line : json.loads(line))\
        .map(lambda line : (line['business_id'],line['state']))\
        .filter(lambda values : (values[1] == "NV"))\
        .map(lambda values : values[0])\
        .collect()
        
    review_rdd = sc.textFile(review_input_json)\
        .map(lambda line : json.loads(line))\
        .map(lambda line : (line['review_id'],line['business_id']))\
        .filter(lambda line : line[1] in business_rdd)\
        .collect()
        
    with open(output_file_path,'w') as output_file:
        csv_output_file = csv.writer(output_file)
        csv_output_file.writerow(['user_id','business_id'])
        for row in review_rdd:
            csv_output_file.writerow(row)