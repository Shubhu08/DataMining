#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 27 13:52:54 2021

@author: shubhashreedash
"""

import sys
import json
from pyspark import SparkContext


if __name__ == '__main__':
    review_input_json = sys.argv[1]
    business_input_json = sys.argv[2]
    output_file_path = sys.argv[3]
    spark = sys.argv[4]
    n = int(sys.argv[5])
    result = {}
    if spark == "spark":
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")
        sc.setSystemProperty('spark.driver.memory', '4g')
        sc.setSystemProperty('spark.executor.memory', '4g')
        
        review_rdd = sc.textFile(review_input_json)\
            .map(lambda line : json.loads(line))\
            .map(lambda line : (line['business_id'],(float(line['stars']),1)))\
            .reduceByKey(lambda vals,val : (vals[0]+val[0], vals[1]+val[1]))
        
        #print("Review : ",review_rdd.collect()) 
           
        business_rdd = sc.textFile(business_input_json)\
            .map(lambda line : json.loads(line))\
            .map(lambda line : (line['business_id'],line['categories']))\
            .filter(lambda values : (values[1] != None) and (values[1] != ""))\
            .flatMapValues(lambda values : [value.strip() for value in values.split(',')])
        
        #print("Business : ",business_rdd.collect()) 
        
        joined_rdd = business_rdd.join(review_rdd)\
            .map(lambda key_val : key_val[1])\
            .reduceByKey(lambda vals,val : (vals[0]+val[0], vals[1]+val[1]))\
            .mapValues(lambda value : round(value[0]/value[1],1))\
            .takeOrdered(n , key = lambda val : (-val[1] , val[0]))
        
        result['result'] = joined_rdd
        print(result)
        
    else:
        business_id_category_dict = {}
        with open(business_input_json,'r') as business_json:
            for line in business_json:
                line_dict = json.loads(line)
                categories = line_dict['categories']
                if categories == None or categories == "":
                    continue
                else:
                    business_id_category_dict[line_dict['business_id']] = [category.strip() for category in categories.split(',')]

        category_review = {}                
        with open(review_input_json) as review_json:
            for line in review_json:
                line_dict = json.loads(line)
                business_id = line_dict['business_id']
                if business_id in business_id_category_dict.keys():
                    for category in business_id_category_dict[business_id]:
                        category_review[category] = (category_review.get(category,(0,0))[0] + float(line_dict['stars']), category_review.get(category,(0,0))[1]+1)
        
        for category in category_review.keys():
            category_review[category] = round(float(category_review.get(category)[0]/category_review.get(category)[1]),1)
        
            
        result['result'] = sorted(category_review.items(), key = lambda kv:(-kv[1], kv[0]))[:n]
        print(result)
        
        
    with open(output_file_path, 'w+') as output_file:
        json.dump(result, output_file)
    output_file.close()