#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 29 03:02:06 2021

@author: shubhashreedash
"""

import sys
import json
from pyspark import SparkContext

if __name__ == '__main__':
    review_input_json = sys.argv[1]
    output_file_path = sys.argv[2]
    partition_type = sys.argv[3]
    n_partitions = int(sys.argv[4])
    n = int(sys.argv[5])
    result = {}

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    sc.setSystemProperty('spark.driver.memory', '4g')
    sc.setSystemProperty('spark.executor.memory', '4g')
    
    
    result = {}
    
    review_rdd = sc.textFile(review_input_json).map(lambda line : json.loads(line))\
        .map(lambda line : (line['business_id'],1))
    
    if partition_type == "customized":
        review_rdd = review_rdd.partitionBy(n_partitions, lambda item : hash(item[0]))
       
        
    result['n_partitions'] = review_rdd.getNumPartitions()
    result['n_items'] = review_rdd.glom().map(len).collect()
    result['result'] = review_rdd.reduceByKey(lambda val,val1 : val+val1)\
        .filter(lambda line : line[1] > n)\
        .collect()
        
    print("Result : ",result)
    with open(output_file_path, 'w+') as output_file:
        json.dump(result, output_file)
    output_file.close()